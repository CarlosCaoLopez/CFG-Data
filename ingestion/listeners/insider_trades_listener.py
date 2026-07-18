# L2: Insider Trades Listener
#
# Monitors wallets flagged as insiders by T3:
# 1. Command listener (XREADGROUP) → receives ADD/UPDATE/REMOVE wallet commands
# 2. Trade polling (every 30s) → fetches new trades for monitored wallets
# 3. Category filtering → only publishes trades in matching categories
#
# On new insider trades: normalize, publish to 'insider_trades' stream

import asyncio
import json
import structlog
from datetime import datetime, UTC
from dataclasses import dataclass, field

import redis.asyncio as redis

from ingestion.config import get_settings
from ingestion.publisher import RedisPublisher
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.config_store import CategoriesStore
from ingestion.interests_registry import InterestsRegistry
from contracts.raw_market_data import InsiderTradeCommand

logger = structlog.get_logger()


@dataclass
class MonitoredWallet:
    """State for a monitored insider wallet."""
    wallet: str
    categories_insider: list[str]
    market_ids_insider: list[str] | None = None
    last_seen_ts: datetime | None = None
    trades_published: int = 0


class InsiderTradesListener:
    """
    Listener 2 (L2): Monitors insider wallets commanded by T3.

    Receives ADD_WALLET / UPDATE_CATEGORIES / REMOVE_WALLET commands
    from the 'insider_commands' Redis stream, then continuously polls
    each monitored wallet's trades, filtering by CATEGORIAS_DE_INTERES
    intersection with the wallet's categories_insider.

    Publishes new insider trades to 'insider_trades' stream.
    """

    def __init__(
        self,
        publisher: RedisPublisher | None = None,
        trades_collector: TradesCollector | None = None,
        markets_collector: MarketsCollector | None = None,
        normalizer: DataNormalizer | None = None,
        registry: "InterestsRegistry | None" = None,
    ):
        self.settings = get_settings()
        self.publisher = publisher or RedisPublisher()
        self.trades_collector = trades_collector or TradesCollector()
        self.markets_collector = markets_collector or MarketsCollector()
        self.normalizer = normalizer or DataNormalizer(
            categorias_de_interes=self.settings.categorias_de_interes,
        )
        self.logger = logger.bind(component="l2_insider_trades")

        # Monitored wallets: wallet_address → MonitoredWallet
        self._monitored_wallets: dict[str, MonitoredWallet] = {}

        # Market metadata cache: condition_id → raw market dict
        self._market_cache: dict[str, dict] = {}

        # Redis client for XREADGROUP
        self._redis: redis.Redis | None = None

        # State
        self._running = False
        self._stats = {
            "commands_received": 0,
            "wallets_added": 0,
            "wallets_removed": 0,
            "trades_polled": 0,
            "trades_published": 0,
        }

        # Shared category filter (see InterestsRegistry). L2 uses it only to keep
        # its normalizer's tag→category derivation in sync — the per-wallet
        # categories_insider (from T3) remain the actual monitoring filter.
        self._owns_registry = registry is None
        self.registry = registry or InterestsRegistry(
            CategoriesStore(self.settings.config_store_path),
            self.settings.categorias_de_interes,
        )
        self.registry.subscribe(self._on_interests_changed)

    def _on_interests_changed(self, new_categories: list[str]) -> None:
        """Registry callback: keep the normalizer's derivation filter live."""
        self.normalizer.set_interests(new_categories)
        self.logger.info("interests_applied", new_interests=new_categories)

    @property
    def monitored_count(self) -> int:
        return len(self._monitored_wallets)

    @property
    def stats(self) -> dict:
        return {**self._stats, "monitored_wallets": self.monitored_count}

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection for command stream."""
        if self._redis is None:
            self._redis = redis.from_url(self.settings.redis_url)
        return self._redis

    # ================================================================
    # MONITORED WALLETS PERSISTENCE (survive redeploys)
    # ================================================================

    async def _load_monitored_wallets(self) -> None:
        """Rehydrate _monitored_wallets from Redis (mirrors L1's tracked markets)."""
        persisted = await self.publisher.load_monitored_wallets()
        for wallet, data in persisted.items():
            last_seen_ts = self.normalizer._parse_datetime_optional(
                data.get("last_seen_ts")
            )
            self._monitored_wallets[wallet] = MonitoredWallet(
                wallet=wallet,
                categories_insider=data.get("categories_insider", []),
                market_ids_insider=data.get("market_ids_insider"),
                last_seen_ts=last_seen_ts,
            )
        if persisted:
            self.logger.info(
                "monitored_wallets_rehydrated", count=len(persisted)
            )

    def _serialize_monitored(self, info: MonitoredWallet) -> dict:
        return {
            "categories_insider": info.categories_insider,
            "market_ids_insider": info.market_ids_insider,
            "last_seen_ts": info.last_seen_ts.isoformat() if info.last_seen_ts else None,
        }

    async def run(self) -> None:
        """Run command listener + trade polling concurrently."""
        self._running = True
        self.logger.info("l2_starting")

        # Load persisted category filter (survives redeploys). If a shared
        # registry was injected, main.py loads it once for L1 + L2.
        if self._owns_registry:
            await self.registry.load()

        # Rehydrate monitored wallets (survives redeploys). Without this,
        # ADD_WALLET commands only fire once at detection time and a
        # docker compose down/up silently stops tracking every insider
        # already detected.
        await self._load_monitored_wallets()

        # Ensure consumer group exists
        await self._ensure_consumer_group()

        await asyncio.gather(
            self._command_listener_loop(),
            self._trade_polling_loop(),
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        self.logger.info("l2_stopping", stats=self.stats)
        self._running = False
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    # ================================================================
    # CONSUMER GROUP SETUP
    # ================================================================

    async def _ensure_consumer_group(self) -> None:
        """Create consumer group for insider_commands stream if it doesn't exist."""
        r = await self._get_redis()
        stream = self.settings.l2_commands_stream
        group = self.settings.l2_commands_consumer_group

        try:
            await r.xgroup_create(
                stream, group, id="0", mkstream=True
            )
            self.logger.info(
                "consumer_group_created",
                stream=stream,
                group=group,
            )
        except redis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                self.logger.debug(
                    "consumer_group_already_exists",
                    stream=stream,
                    group=group,
                )
            else:
                raise

    # ================================================================
    # COMMAND LISTENER
    # ================================================================

    async def _command_listener_loop(self) -> None:
        """
        Listen for commands from T3 via XREADGROUP on 'insider_commands'.
        Blocking read with 5s timeout to allow graceful shutdown.
        """
        r = await self._get_redis()
        stream = self.settings.l2_commands_stream
        group = self.settings.l2_commands_consumer_group
        consumer = "l2_worker_1"

        self.logger.info(
            "command_listener_started",
            stream=stream,
            group=group,
        )

        while self._running:
            try:
                # Blocking read with 5s timeout
                results = await r.xreadgroup(
                    group, consumer,
                    {stream: ">"},
                    count=10,
                    block=5000,
                )

                if not results:
                    continue

                for stream_name, messages in results:
                    for msg_id, data in messages:
                        try:
                            await self._process_command(msg_id, data)
                            # ACK after successful processing
                            await r.xack(stream, group, msg_id)
                        except Exception as e:
                            self.logger.error(
                                "command_processing_failed",
                                msg_id=msg_id,
                                error=str(e),
                            )

            except redis.ConnectionError as e:
                self.logger.error("command_listener_connection_error", error=str(e))
                if not self._running:
                    break
                await asyncio.sleep(5)
            except Exception as e:
                self.logger.error("command_listener_error", error=str(e))
                if not self._running:
                    break
                await asyncio.sleep(1)

    async def _process_command(self, msg_id: bytes, data: dict) -> None:
        """
        Process a command from the insider_commands stream.

        Expected data fields:
        - payload: JSON string of InsiderTradeCommand
        OR individual fields:
        - wallet, categories_insider (JSON list), command_type
        """
        self._stats["commands_received"] += 1

        # Decode bytes to str if needed
        decoded = {}
        for k, v in data.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = val

        # Try parsing from payload field first, then from individual fields
        if "payload" in decoded:
            command = InsiderTradeCommand.model_validate_json(decoded["payload"])
        else:
            categories = decoded.get("categories_insider", "[]")
            if isinstance(categories, str):
                categories = json.loads(categories)

            market_ids = decoded.get("market_ids_insider")
            if market_ids and isinstance(market_ids, str):
                market_ids = json.loads(market_ids)

            command = InsiderTradeCommand(
                wallet=decoded["wallet"],
                categories_insider=categories,
                market_ids_insider=market_ids,
                command_type=decoded.get("command_type", "ADD_WALLET"),
            )

        wallet = command.wallet.lower()

        if command.command_type == "ADD_WALLET":
            self._monitored_wallets[wallet] = MonitoredWallet(
                wallet=wallet,
                categories_insider=command.categories_insider,
                market_ids_insider=command.market_ids_insider,
            )
            await self.publisher.save_monitored_wallet(
                wallet, self._serialize_monitored(self._monitored_wallets[wallet])
            )
            self._stats["wallets_added"] += 1
            self.logger.info(
                "wallet_added",
                wallet=wallet[:10] + "...",
                categories=command.categories_insider,
                total_monitored=self.monitored_count,
            )

        elif command.command_type == "UPDATE_CATEGORIES":
            if wallet in self._monitored_wallets:
                self._monitored_wallets[wallet].categories_insider = (
                    command.categories_insider
                )
                if command.market_ids_insider:
                    self._monitored_wallets[wallet].market_ids_insider = (
                        command.market_ids_insider
                    )
                self.logger.info(
                    "wallet_categories_updated",
                    wallet=wallet[:10] + "...",
                    categories=command.categories_insider,
                )
            else:
                # Treat as ADD if not already monitored
                self._monitored_wallets[wallet] = MonitoredWallet(
                    wallet=wallet,
                    categories_insider=command.categories_insider,
                    market_ids_insider=command.market_ids_insider,
                )
                self._stats["wallets_added"] += 1
            await self.publisher.save_monitored_wallet(
                wallet, self._serialize_monitored(self._monitored_wallets[wallet])
            )

        elif command.command_type == "REMOVE_WALLET":
            if wallet in self._monitored_wallets:
                del self._monitored_wallets[wallet]
                await self.publisher.remove_monitored_wallet(wallet)
                self._stats["wallets_removed"] += 1
                self.logger.info(
                    "wallet_removed",
                    wallet=wallet[:10] + "...",
                    total_monitored=self.monitored_count,
                )

    # ================================================================
    # TRADE POLLING
    # ================================================================

    async def _trade_polling_loop(self) -> None:
        """Periodically poll trades for all monitored wallets."""
        # Initial delay to let command listener populate wallets
        await asyncio.sleep(5)

        while self._running:
            if self._monitored_wallets:
                try:
                    await self._poll_all_wallets()
                except Exception as e:
                    self.logger.error("trade_polling_error", error=str(e))

            await asyncio.sleep(self.settings.l2_poll_interval_seconds)

    async def _poll_all_wallets(self) -> None:
        """Poll trades for each monitored wallet."""
        # Copy to avoid mutation during iteration
        wallets = list(self._monitored_wallets.items())

        self.logger.debug(
            "polling_wallet_trades",
            wallets_count=len(wallets),
        )

        for wallet_addr, info in wallets:
            if not self._running:
                break

            try:
                await self._poll_wallet_trades(wallet_addr, info)
            except Exception as e:
                self.logger.warning(
                    "wallet_poll_failed",
                    wallet=wallet_addr[:10] + "...",
                    error=str(e),
                )

    async def _poll_wallet_trades(
        self, wallet: str, info: MonitoredWallet
    ) -> None:
        """
        Fetch recent trades for a wallet, filter by categories, publish new ones.
        """
        trades_raw = []
        try:
            async for trade in self.trades_collector.fetch_trades_for_wallet(
                wallet, limit=100
            ):
                trades_raw.append(trade)
        except Exception as e:
            self.logger.warning(
                "fetch_trades_for_wallet_failed",
                wallet=wallet[:10] + "...",
                error=str(e),
            )
            return

        self.logger.debug(
            "wallet_trades_fetched",
            wallet=wallet[:10] + "...",
            trades_count=len(trades_raw),
        )

        if not trades_raw:
            return

        self._stats["trades_polled"] += len(trades_raw)

        # Filter trades newer than last_seen_ts
        new_trades = []
        for trade_raw in trades_raw:
            ts = trade_raw.get("timestamp")
            if ts and info.last_seen_ts:
                trade_ts = self.normalizer._parse_datetime(ts)
                if trade_ts <= info.last_seen_ts:
                    continue
            new_trades.append(trade_raw)

        if not new_trades:
            return

        # Filter by market categories
        matching_trades = []
        for trade_raw in new_trades:
            condition_id = trade_raw.get("conditionId") or trade_raw.get("market")
            if not condition_id:
                continue

            if await self._market_matches_categories(
                condition_id, info.categories_insider
            ):
                try:
                    normalized = self.normalizer.normalize_trade(
                        trade_raw, condition_id
                    )
                    matching_trades.append(normalized)
                except Exception as e:
                    self.logger.debug(
                        "trade_normalization_skipped",
                        error=str(e),
                    )

        if matching_trades:
            # Publish insider trades
            await self.publisher.publish_insider_trades(
                wallet=wallet,
                trades=matching_trades,
                categories_insider=info.categories_insider,
            )
            self._stats["trades_published"] += len(matching_trades)
            info.trades_published += len(matching_trades)

            self.logger.info(
                "insider_trades_published",
                wallet=wallet[:10] + "...",
                trades_count=len(matching_trades),
                categories=info.categories_insider,
            )

        # Update last_seen_ts to the most recent trade timestamp
        if new_trades:
            latest_ts = max(
                self.normalizer._parse_datetime(t.get("timestamp"))
                for t in new_trades
                if t.get("timestamp")
            )
            info.last_seen_ts = latest_ts
            # Persist so a restart doesn't re-poll from scratch (mitigated by
            # ON CONFLICT DO NOTHING downstream, but wastes Data API calls).
            await self.publisher.save_monitored_wallet(
                wallet, self._serialize_monitored(info)
            )

    # ================================================================
    # CATEGORY MATCHING
    # ================================================================

    async def _market_matches_categories(
        self, condition_id: str, categories_insider: list[str]
    ) -> bool:
        """
        Check if a market belongs to any of the wallet's insider categories.
        Uses cache to avoid repeated API calls.
        """
        # Check cache
        if condition_id not in self._market_cache:
            market = await self.markets_collector.fetch_market_by_id(condition_id)
            if not market:
                return False
            # Enrich with Events API for tags
            events = market.get("events", [])
            if events and isinstance(events, list) and len(events) > 0:
                event_id = events[0].get("id")
                if event_id:
                    event_data = await self.markets_collector.fetch_event_by_id(
                        str(event_id)
                    )
                    if event_data:
                        event_category = event_data.get("category")
                        if event_category:
                            market["category"] = event_category
                        event_tags = event_data.get("tags", [])
                        if event_tags and market.get("events"):
                            market["events"][0]["tags"] = event_tags

            self._market_cache[condition_id] = market

        market = self._market_cache[condition_id]

        # Build set of market's categories/tags (lowercase)
        market_labels = set()

        category = (market.get("category") or "").lower()
        if category:
            market_labels.add(category)

        for event in market.get("events", []):
            if not isinstance(event, dict):
                continue
            for tag in event.get("tags", []):
                if isinstance(tag, dict):
                    label = (tag.get("label") or tag.get("slug") or "").lower()
                elif isinstance(tag, str):
                    label = tag.lower()
                else:
                    continue
                if label:
                    market_labels.add(label)

        # Check intersection with wallet's insider categories
        insider_set = set(c.lower() for c in categories_insider)
        return bool(market_labels & insider_set)
