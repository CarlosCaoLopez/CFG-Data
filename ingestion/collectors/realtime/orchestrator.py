# Hybrid orchestrator for WebSocket + Polling realtime listener
#
# Coordinates:
# 1. WebSocket as primary source
# 2. Polling as fallback when WebSocket fails
# 3. Catch-up of missed events during reconnections
# 4. Deduplication of events
# 5. Market metadata enrichment (category, tags) via cache
# 6. Publishing to Redis

import asyncio
from collections import OrderedDict
from datetime import datetime, UTC
from typing import Callable, Awaitable
import structlog

from ingestion.config import get_settings
from ingestion.publisher import RedisPublisher
from ingestion.http_client import HTTPClient
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from contracts.raw_market_data import RawTrade, RawMarketMetadata

from .websocket_listener import WebSocketListener, ConnectionState
from .polling_listener import PollingListener
from .event_decoder import DecodedOrderFilled

logger = structlog.get_logger()


class LRUCache:
    """Simple LRU cache for deduplication"""

    def __init__(self, max_size: int):
        self.max_size = max_size
        self._cache: OrderedDict[str, datetime] = OrderedDict()

    def contains(self, key: str) -> bool:
        """Check if key exists (and move to end as most recently used)"""
        if key in self._cache:
            self._cache.move_to_end(key)
            return True
        return False

    def add(self, key: str) -> None:
        """Add key to cache (evicts oldest if over capacity)"""
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            self._cache[key] = datetime.now(UTC)
            # Evict oldest entries if over capacity
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def __len__(self) -> int:
        return len(self._cache)


class RealtimeOrchestrator:
    """
    Main orchestrator for realtime ingestion.

    Strategy:
    1. Connect via WebSocket (latency ~2s)
    2. If WebSocket fails, activate polling fallback
    3. When WebSocket reconnects, catch-up missed events
    4. Deduplicate events by tx_hash + log_index
    5. Publish each unique event to Redis

    Usage:
        async with RealtimeOrchestrator() as orchestrator:
            await orchestrator.run()
    """

    def __init__(
        self,
        publisher: RedisPublisher | None = None,
        http_client: HTTPClient | None = None,
    ):
        self.settings = get_settings()
        self.publisher = publisher or RedisPublisher()
        self.http_client = http_client or HTTPClient()
        self.logger = logger.bind(component="realtime_orchestrator")

        # Market metadata enrichment
        self._markets_collector = MarketsCollector()
        self._normalizer = DataNormalizer()
        self._market_cache: dict[str, RawMarketMetadata] = {}
        self._cache_misses: set[str] = set()  # Token IDs not found in API

        # Deduplication cache
        self._seen_events = LRUCache(self.settings.dedup_cache_size)

        # Listeners
        self._ws_listener: WebSocketListener | None = None
        self._polling_listener: PollingListener | None = None

        # State tracking
        self._running = False
        self._using_fallback = False
        self._last_ws_block: int = 0
        self._events_processed = 0
        self._events_deduplicated = 0
        self._cache_hits = 0
        self._api_enrichments = 0

        # Tasks
        self._ws_task: asyncio.Task | None = None
        self._polling_task: asyncio.Task | None = None

    @property
    def events_processed(self) -> int:
        """Total events processed and published"""
        return self._events_processed

    @property
    def events_deduplicated(self) -> int:
        """Events skipped due to deduplication"""
        return self._events_deduplicated

    @property
    def using_fallback(self) -> bool:
        """Whether currently using polling fallback"""
        return self._using_fallback

    @property
    def cache_stats(self) -> dict:
        """Market metadata cache statistics"""
        return {
            "cache_size": len(self._market_cache),
            "cache_hits": self._cache_hits,
            "api_enrichments": self._api_enrichments,
            "not_found": len(self._cache_misses),
        }

    async def run(self) -> None:
        """
        Run the orchestrator.

        Runs indefinitely until stop() is called.
        """
        self._running = True
        self.logger.info("orchestrator_starting")

        # Initialize listeners
        self._ws_listener = WebSocketListener(
            on_event=self._handle_event,
            on_connection_lost=self._on_ws_connection_lost,
        )

        self._polling_listener = PollingListener(
            on_event=self._handle_event,
            http_client=self.http_client,
        )

        # Start WebSocket as primary
        self._ws_task = asyncio.create_task(
            self._run_websocket(), name="websocket_listener"
        )

        # Wait for completion (only when stop() is called or fails permanently)
        try:
            await self._ws_task
        except asyncio.CancelledError:
            pass

        self.logger.info(
            "orchestrator_stopped",
            events_processed=self._events_processed,
            events_deduplicated=self._events_deduplicated,
            cache_stats=self.cache_stats,
        )

    async def stop(self) -> None:
        """Stop the orchestrator gracefully"""
        self.logger.info("orchestrator_stopping")
        self._running = False

        # Stop listeners
        if self._ws_listener:
            await self._ws_listener.stop()

        if self._polling_listener:
            await self._polling_listener.stop()

        # Cancel tasks
        for task in [self._ws_task, self._polling_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _run_websocket(self) -> None:
        """Run WebSocket listener with fallback handling"""
        while self._running:
            try:
                # Save last block before potential disconnection
                if self._ws_listener and self._ws_listener.last_block_seen > 0:
                    self._last_ws_block = self._ws_listener.last_block_seen

                await self._ws_listener.start()

            except Exception as e:
                self.logger.error("websocket_failed", error=str(e))

                if not self._running:
                    break

                # Check if WS listener has given up
                if self._ws_listener.state == ConnectionState.FAILED:
                    await self._activate_fallback()
                    break

    async def _activate_fallback(self) -> None:
        """Activate polling as fallback"""
        if self._using_fallback:
            return

        self._using_fallback = True
        self.logger.warning("activating_polling_fallback")

        # Catch-up missed events
        if self._last_ws_block > 0:
            self.logger.info(
                "catching_up_missed_events",
                from_block=self._last_ws_block,
            )
            try:
                events_caught = await self._polling_listener.catch_up(
                    from_block=self._last_ws_block
                )
                self.logger.info("catch_up_completed", events_caught=events_caught)
            except Exception as e:
                self.logger.error("catch_up_failed", error=str(e))

        # Start polling loop
        self._polling_task = asyncio.create_task(
            self._polling_listener.start(), name="polling_listener"
        )

        try:
            await self._polling_task
        except asyncio.CancelledError:
            pass

    async def _on_ws_connection_lost(self) -> None:
        """Callback when WebSocket connection is lost permanently"""
        self.logger.warning("websocket_connection_lost_permanently")
        await self._activate_fallback()

    async def _get_market_metadata(self, token_id: str) -> RawMarketMetadata | None:
        """
        Get market metadata for a token ID, using cache when possible.

        Args:
            token_id: The outcome token ID from the on-chain event

        Returns:
            RawMarketMetadata with category/tags, or None if not found
        """
        # Check cache first
        if token_id in self._market_cache:
            self._cache_hits += 1
            return self._market_cache[token_id]

        # Skip if we already know this token_id is not in the API
        if token_id in self._cache_misses:
            return None

        # Fetch from Gamma API
        try:
            raw_market = await self._markets_collector.fetch_market_by_token_id(token_id)

            if raw_market:
                # Normalize using existing normalizer (extracts category, tags)
                metadata = self._normalizer.normalize_market(raw_market)
                self._market_cache[token_id] = metadata
                self._api_enrichments += 1

                self.logger.debug(
                    "market_metadata_enriched",
                    token_id=token_id[:20] + "...",
                    market_id=metadata.market_id[:20] + "...",
                    category=metadata.category,
                    tags_count=len(metadata.tags) if metadata.tags else 0,
                )

                return metadata
            else:
                # Mark as not found to avoid repeated API calls
                self._cache_misses.add(token_id)
                return None

        except Exception as e:
            self.logger.warning(
                "market_metadata_fetch_failed",
                token_id=token_id[:20] + "...",
                error=str(e),
            )
            return None

    async def _handle_event(self, event: DecodedOrderFilled) -> None:
        """
        Process a decoded event.

        1. Deduplicate by tx_hash + log_index
        2. Enrich with market metadata (category, tags) from cache/API
        3. Convert to RawTrade
        4. Publish to Redis
        """
        # Deduplication key: tx_hash + log_index (one tx can have multiple logs)
        dedup_key = f"{event.tx_hash}:{event.log_index}"

        if self._seen_events.contains(dedup_key):
            self._events_deduplicated += 1
            self.logger.debug(
                "duplicate_event_skipped",
                tx_hash=event.tx_hash[:16] + "...",
            )
            return

        self._seen_events.add(dedup_key)

        # Get token ID for market lookup
        token_id = str(
            event.taker_asset_id if event.is_maker_usdc else event.maker_asset_id
        )

        # Try to get enriched market metadata (with category, tags)
        market_metadata = await self._get_market_metadata(token_id)

        if market_metadata is None:
            # Fallback to minimal metadata if API lookup fails
            market_metadata = self._create_minimal_market_metadata(event)

        # Convert to RawTrade (use real market_id if we have it)
        trade = self._to_raw_trade(event, market_metadata.market_id)

        # Publish to Redis
        try:
            job_id = await self.publisher.publish_realtime_trade(trade, market_metadata)
            self._events_processed += 1

            self.logger.info(
                "event_published",
                job_id=job_id[:20] + "..." if len(job_id) > 20 else job_id,
                tx_hash=event.tx_hash[:16] + "...",
                maker=event.maker[:10] + "...",
                side=event.side,
                usdc=f"{event.maker_amount if event.is_maker_usdc else event.taker_amount:.2f}",
                category=market_metadata.category,
            )

        except Exception as e:
            self.logger.error(
                "publish_failed",
                error=str(e),
                tx_hash=event.tx_hash[:16] + "...",
            )

    def _to_raw_trade(self, event: DecodedOrderFilled, market_id: str | None = None) -> RawTrade:
        """Convert DecodedOrderFilled to RawTrade contract"""
        # Use maker as the wallet (order creator)
        wallet = event.maker

        # Determine USDC size
        # If maker gives USDC (BUY), size is maker_amount
        # If maker gives tokens (SELL), size is taker_amount (what they receive)
        size_usdc = event.maker_amount if event.is_maker_usdc else event.taker_amount

        # Token ID of the market (used as fallback if no market_id provided)
        token_id = event.taker_asset_id if event.is_maker_usdc else event.maker_asset_id

        # Calculate approximate price
        # price = USDC / tokens
        tokens = event.taker_amount if event.is_maker_usdc else event.maker_amount
        price = size_usdc / tokens if tokens > 0 else 0.0
        price = min(max(price, 0.0), 1.0)  # Clamp to [0, 1]

        return RawTrade(
            tx_hash=event.tx_hash,
            wallet=wallet,
            market_id=market_id or str(token_id),  # Use real market_id if available
            timestamp=datetime.now(UTC),  # Ideally use block timestamp
            side=event.side,
            outcome="Yes",  # Will be determined with more context
            price=price,
            size_usdc=size_usdc,
            size_tokens=tokens,
        )

    def _create_minimal_market_metadata(
        self, event: DecodedOrderFilled
    ) -> RawMarketMetadata:
        """Create minimal metadata for the trade (Worker will enrich)"""
        token_id = event.taker_asset_id if event.is_maker_usdc else event.maker_asset_id

        return RawMarketMetadata(
            market_id=str(token_id),
            question="[Pending enrichment from on-chain event]",
            end_date=datetime.now(UTC),  # Placeholder
            closed=False,
            outcome_prices=None,
            volume_usdc=0.0,
            category=None,
            tags=None,
        )

    async def __aenter__(self) -> "RealtimeOrchestrator":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
        if self.http_client:
            await self.http_client.close()
