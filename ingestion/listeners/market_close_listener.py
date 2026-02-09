# L1: Market Close Listener
#
# Hybrid discovery (WebSocket + Gamma API):
# 1. WebSocket on-chain events → discover markets with trades in real-time
# 2. Gamma API polling (every 15 min) → catch-all for markets without recent trades
# 3. Close detection (every 60s) → check tracked markets for closure
#
# On market close: fetch ALL trades, wallet ages, normalize, publish batch

import asyncio
import structlog
from datetime import datetime, UTC

from ingestion.config import get_settings
from ingestion.publisher import RedisPublisher
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.wallet_age_collector import WalletAgeCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.collectors.realtime.websocket_listener import WebSocketListener
from ingestion.collectors.realtime.polling_listener import PollingListener
from ingestion.collectors.realtime.event_decoder import DecodedOrderFilled
from contracts.raw_market_data import RawMarketMetadata

logger = structlog.get_logger()


class MarketCloseListener:
    """
    Listener 1 (L1): Monitors markets matching CATEGORIAS_DE_INTERES.

    When a market closes, fetches ALL trades, wallet ages, and publishes
    a complete MarketDataBatch to the 'closed_markets' Redis stream.

    Discovery strategy (hybrid):
    - WebSocket: Real-time discovery from on-chain OrderFilled events
    - Gamma API: Periodic polling for open markets (catch-all)

    Close detection: Periodic polling of tracked markets via Gamma API.
    """

    def __init__(
        self,
        publisher: RedisPublisher | None = None,
        markets_collector: MarketsCollector | None = None,
        trades_collector: TradesCollector | None = None,
        wallet_age_collector: WalletAgeCollector | None = None,
        normalizer: DataNormalizer | None = None,
    ):
        self.settings = get_settings()
        self.publisher = publisher or RedisPublisher()
        self.markets_collector = markets_collector or MarketsCollector()
        self.trades_collector = trades_collector or TradesCollector()
        self.wallet_age_collector = wallet_age_collector or WalletAgeCollector()
        self.normalizer = normalizer or DataNormalizer()
        self.logger = logger.bind(component="l1_market_close")

        # Tracked markets: condition_id → {metadata dict, tags, category}
        self._tracked_markets: dict[str, dict] = {}
        # Already processed (closed and published)
        self._processed_markets: set[str] = set()

        # Market metadata cache for WebSocket discovery (token_id → metadata)
        self._market_cache: dict[str, RawMarketMetadata] = {}
        self._cache_misses: set[str] = set()

        # Categories of interest (lowercased for comparison)
        self._interests = set(
            s.lower() for s in self.settings.categorias_de_interes
        )

        # WebSocket listener for on-chain discovery
        self._ws_listener: WebSocketListener | None = None
        self._polling_listener: PollingListener | None = None

        # State
        self._running = False
        self._stats = {
            "markets_discovered_ws": 0,
            "markets_discovered_gamma": 0,
            "markets_closed_published": 0,
            "markets_filtered_out": 0,
        }

    @property
    def tracked_count(self) -> int:
        return len(self._tracked_markets)

    @property
    def stats(self) -> dict:
        return {**self._stats, "tracked_markets": self.tracked_count}

    async def run(self) -> None:
        """Run all three loops concurrently until stopped."""
        self._running = True
        self.logger.info(
            "l1_starting",
            categorias_de_interes=list(self._interests),
        )

        await asyncio.gather(
            self._ws_discovery_loop(),
            self._gamma_discovery_loop(),
            self._close_detection_loop(),
        )

    async def stop(self) -> None:
        """Graceful shutdown."""
        self.logger.info("l1_stopping", stats=self.stats)
        self._running = False
        if self._ws_listener:
            await self._ws_listener.stop()
        if self._polling_listener:
            await self._polling_listener.stop()

    # ================================================================
    # HYBRID DISCOVERY
    # ================================================================

    async def _ws_discovery_loop(self) -> None:
        """
        WebSocket on-chain discovery: listen to OrderFilled events,
        enrich market metadata, filter by categories, track matching markets.
        """
        self._ws_listener = WebSocketListener(
            on_event=self._handle_onchain_event,
            on_connection_lost=self._on_ws_lost,
        )
        self._polling_listener = PollingListener(
            on_event=self._handle_onchain_event,
        )

        while self._running:
            try:
                await self._ws_listener.start()
            except Exception as e:
                self.logger.error("ws_discovery_error", error=str(e))
                if not self._running:
                    break
                # If WebSocket failed permanently, use polling fallback
                if self._ws_listener.state.value == "failed":
                    self.logger.warning("ws_failed_switching_to_polling")
                    try:
                        await self._polling_listener.start()
                    except Exception as pe:
                        self.logger.error("polling_discovery_error", error=str(pe))
                    break

    async def _on_ws_lost(self) -> None:
        """Callback when WebSocket connection is lost permanently."""
        self.logger.warning("ws_connection_lost_for_discovery")

    async def _handle_onchain_event(self, event: DecodedOrderFilled) -> None:
        """
        Handle an on-chain OrderFilled event for market discovery.
        Instead of publishing the trade, we just track the market.
        """
        # Get token_id for market lookup
        token_id = str(
            event.taker_asset_id if event.is_maker_usdc else event.maker_asset_id
        )

        # Skip if we already know this token
        if token_id in self._cache_misses:
            return

        # Check cache first
        if token_id in self._market_cache:
            metadata = self._market_cache[token_id]
            # Already tracked or processed
            if (
                metadata.market_id in self._tracked_markets
                or metadata.market_id in self._processed_markets
            ):
                return
            # Track it if it matches categories
            if self._metadata_matches_categories(metadata):
                self._track_market_from_metadata(metadata)
            return

        # Fetch and enrich market metadata
        try:
            raw_market = await self.markets_collector.fetch_market_by_token_id(
                token_id
            )
            if not raw_market:
                self._cache_misses.add(token_id)
                return

            metadata = self.normalizer.normalize_market(raw_market)
            self._market_cache[token_id] = metadata

            # Skip already closed markets
            if metadata.closed:
                return

            condition_id = metadata.market_id
            if (
                condition_id in self._tracked_markets
                or condition_id in self._processed_markets
            ):
                return

            if self._metadata_matches_categories(metadata):
                self._track_market_from_metadata(metadata)
                self._stats["markets_discovered_ws"] += 1
            else:
                self._stats["markets_filtered_out"] += 1

        except Exception as e:
            self.logger.debug(
                "ws_discovery_enrich_failed",
                token_id=token_id[:20] + "...",
                error=str(e),
            )

    async def _gamma_discovery_loop(self) -> None:
        """
        Gamma API discovery (catch-all): periodically fetch open markets
        with sufficient volume and filter by CATEGORIAS_DE_INTERES.
        """
        # Initial delay to let WebSocket start first
        await asyncio.sleep(10)

        while self._running:
            try:
                await self._discover_from_gamma()
            except Exception as e:
                self.logger.error("gamma_discovery_error", error=str(e))

            await asyncio.sleep(
                self.settings.l1_gamma_discovery_interval_seconds
            )

    async def _discover_from_gamma(self) -> None:
        """Fetch open markets from Gamma API and track matching ones."""
        discovered = 0

        async for market_raw in self.markets_collector.fetch_open_markets(
            min_volume_usdc=self.settings.l1_min_volume_usdc,
        ):
            condition_id = market_raw.get("conditionId")
            if not condition_id:
                continue

            # Skip already tracked or processed
            if (
                condition_id in self._tracked_markets
                or condition_id in self._processed_markets
            ):
                continue

            # Enrich with Events API for tags/category
            events = market_raw.get("events", [])
            event_id = None
            if events and isinstance(events, list) and len(events) > 0:
                event_id = events[0].get("id")

            if event_id:
                event_data = await self.markets_collector.fetch_event_by_id(
                    str(event_id)
                )
                if event_data:
                    event_category = event_data.get("category")
                    if event_category:
                        market_raw["category"] = event_category
                    event_tags = event_data.get("tags", [])
                    if event_tags and market_raw.get("events"):
                        market_raw["events"][0]["tags"] = event_tags

            # Check categories
            if self._matches_categories(market_raw):
                self._tracked_markets[condition_id] = market_raw
                discovered += 1
                self._stats["markets_discovered_gamma"] += 1
            else:
                self._stats["markets_filtered_out"] += 1

        if discovered > 0:
            self.logger.info(
                "gamma_discovery_completed",
                new_markets=discovered,
                total_tracked=len(self._tracked_markets),
            )

    # ================================================================
    # CLOSE DETECTION
    # ================================================================

    async def _close_detection_loop(self) -> None:
        """Periodically check tracked markets for closure."""
        # Initial delay to let discovery populate markets
        await asyncio.sleep(30)

        while self._running:
            try:
                await self._check_for_closures()
            except Exception as e:
                self.logger.error("close_detection_error", error=str(e))

            await asyncio.sleep(
                self.settings.l1_market_poll_interval_seconds
            )

    async def _check_for_closures(self) -> None:
        """Re-fetch each tracked market and handle closures."""
        if not self._tracked_markets:
            return

        # Copy keys to avoid mutation during iteration
        market_ids = list(self._tracked_markets.keys())

        self.logger.debug(
            "checking_for_closures",
            tracked_count=len(market_ids),
        )

        for condition_id in market_ids:
            if not self._running:
                break

            try:
                market = await self.markets_collector.fetch_market_by_id(
                    condition_id
                )
                if market and market.get("closed"):
                    self.logger.info(
                        "market_close_detected",
                        condition_id=condition_id[:20] + "...",
                        question=market.get("question", "")[:50],
                    )
                    await self._handle_market_close(market)
                    self._processed_markets.add(condition_id)
                    self._tracked_markets.pop(condition_id, None)

            except Exception as e:
                self.logger.warning(
                    "close_check_failed",
                    condition_id=condition_id[:20] + "...",
                    error=str(e),
                )

    async def _handle_market_close(self, market_raw: dict) -> None:
        """
        Full data collection at market close:
        1. Fetch ALL trades for the market
        2. Extract unique wallets
        3. Fetch wallet ages
        4. Normalize to MarketDataBatch
        5. Compute winning_outcome + mark last_trade
        6. Publish to closed_markets stream
        """
        condition_id = market_raw["conditionId"]

        self.logger.info(
            "processing_market_close",
            condition_id=condition_id[:20] + "...",
        )

        # 1. Fetch all trades
        trades_raw = []
        async for trade in self.trades_collector.fetch_trades_for_market(
            condition_id
        ):
            trades_raw.append(trade)

        if not trades_raw:
            self.logger.warning(
                "no_trades_for_closed_market",
                condition_id=condition_id[:20] + "...",
            )
            return

        # 2. Extract unique wallets
        wallets = self.trades_collector.extract_wallets_from_trades(trades_raw)

        # 3. Fetch wallet ages
        wallet_ages_raw = await self.wallet_age_collector.fetch_wallet_ages_batch(
            wallets
        )

        # 4. Normalize
        batch = self.normalizer.normalize_batch(
            market_raw=market_raw,
            trades_raw=trades_raw,
            wallet_ages_raw=wallet_ages_raw,
        )

        # 5. Compute winning_outcome
        batch.metadata.winning_outcome = self.normalizer.compute_winning_outcome(
            batch.metadata.outcome_prices
        )

        # 6. Mark last_trade
        self.normalizer.mark_last_trade(batch.trades)

        # 7. Publish to closed_markets stream
        message_id = await self.publisher.publish_closed_market_batch(batch)
        self._stats["markets_closed_published"] += 1

        self.logger.info(
            "market_close_published",
            message_id=message_id,
            condition_id=condition_id[:20] + "...",
            trades_count=len(batch.trades),
            wallets_count=len(batch.wallet_ages),
            winning_outcome=batch.metadata.winning_outcome,
        )

    # ================================================================
    # CATEGORY FILTERING
    # ================================================================

    def _matches_categories(self, market_raw: dict) -> bool:
        """Check if a raw market dict matches CATEGORIAS_DE_INTERES."""
        # Check direct category field
        category = (market_raw.get("category") or "").lower()
        if category and category in self._interests:
            return True

        # Check tags from events
        events = market_raw.get("events", [])
        for event in events:
            if not isinstance(event, dict):
                continue
            for tag in event.get("tags", []):
                label = ""
                if isinstance(tag, dict):
                    label = (tag.get("label") or tag.get("slug") or "").lower()
                elif isinstance(tag, str):
                    label = tag.lower()
                if label and label in self._interests:
                    return True

        return False

    def _metadata_matches_categories(self, metadata: RawMarketMetadata) -> bool:
        """Check if normalized metadata matches CATEGORIAS_DE_INTERES."""
        if metadata.category and metadata.category.lower() in self._interests:
            return True
        if metadata.tags:
            for tag in metadata.tags:
                if tag.lower() in self._interests:
                    return True
        return False

    def _track_market_from_metadata(self, metadata: RawMarketMetadata) -> None:
        """Add a market to tracked set from normalized metadata."""
        self._tracked_markets[metadata.market_id] = {
            "conditionId": metadata.market_id,
            "question": metadata.question,
            "endDate": metadata.end_date.isoformat(),
            "category": metadata.category,
            "tags": metadata.tags,
        }
        self.logger.debug(
            "market_tracked",
            condition_id=metadata.market_id[:20] + "...",
            category=metadata.category,
            tags=metadata.tags,
        )
