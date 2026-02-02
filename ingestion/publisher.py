# Publisher - Sends MarketDataBatch to Redis

import json
import uuid
import structlog
from datetime import datetime, timezone

import redis.asyncio as redis

from ingestion.config import get_settings
from job_queue.interfaces.producer import QueueProducer
from contracts.raw_market_data import MarketDataBatch, RawTrade, RawMarketMetadata

logger = structlog.get_logger()

# Redis Stream names
STREAM_RAW_DATA = "stream:raw_market_data"
STREAM_REALTIME = "stream:realtime_trades"


class RedisPublisher(QueueProducer):
    """
    Redis Streams implementation of QueueProducer.

    Publishes MarketDataBatch to Redis for processing by Persona B/C.
    Uses Redis Streams for reliable message delivery with acknowledgments.
    """

    def __init__(self):
        self.settings = get_settings()
        self._redis: redis.Redis | None = None

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection"""
        if self._redis is None:
            self._redis = redis.from_url(
                self.settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self._redis

    async def close(self) -> None:
        """Close Redis connection"""
        if self._redis:
            await self._redis.close()
            self._redis = None

    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """
        Publishes a complete batch of market data to Redis Stream.

        Args:
            batch: Complete market data including metadata, trades, and wallet ages

        Returns:
            batch_id: Unique identifier for the published batch
        """
        r = await self._get_redis()

        batch_id = f"batch_{batch.metadata.market_id[:16]}_{uuid.uuid4().hex[:8]}"

        # Serialize batch to JSON
        batch_data = batch.model_dump_json()

        # Publish to Redis Stream
        message = {
            "batch_id": batch_id,
            "market_id": batch.metadata.market_id,
            "type": "market_batch",
            "priority": "normal",
            "trades_count": str(len(batch.trades)),
            "wallets_count": str(len(batch.wallet_ages)),
            "data": batch_data,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            stream_id = await r.xadd(STREAM_RAW_DATA, message)

            logger.info(
                "batch_published",
                batch_id=batch_id,
                stream_id=stream_id,
                market_id=batch.metadata.market_id[:20] + "...",
                trades_count=len(batch.trades),
            )

            return batch_id

        except redis.RedisError as e:
            logger.error(
                "batch_publish_failed",
                batch_id=batch_id,
                error=str(e),
            )
            raise

    async def publish_realtime_trade(
        self,
        trade: RawTrade,
        market: RawMarketMetadata
    ) -> str:
        """
        Publishes a single trade in real-time with high priority.

        Args:
            trade: Individual trade data
            market: Market metadata for the trade

        Returns:
            job_id: Unique identifier for the created job
        """
        r = await self._get_redis()

        job_id = f"rt_{trade.tx_hash[:16]}_{uuid.uuid4().hex[:8]}"

        # Serialize trade and market
        message = {
            "job_id": job_id,
            "type": "realtime_trade",
            "priority": "high",
            "market_id": market.market_id,
            "trade": trade.model_dump_json(),
            "market": market.model_dump_json(),
            "published_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            stream_id = await r.xadd(STREAM_REALTIME, message)

            logger.debug(
                "realtime_trade_published",
                job_id=job_id,
                stream_id=stream_id,
                tx_hash=trade.tx_hash[:20] + "...",
            )

            return job_id

        except redis.RedisError as e:
            logger.error(
                "realtime_trade_publish_failed",
                job_id=job_id,
                error=str(e),
            )
            raise

    async def health_check(self) -> bool:
        """
        Verifies connection with Redis.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            r = await self._get_redis()
            await r.ping()
            return True
        except redis.RedisError as e:
            logger.warning("redis_health_check_failed", error=str(e))
            return False

    async def get_stream_info(self) -> dict:
        """
        Get information about the Redis streams.

        Returns:
            Dict with stream lengths and info
        """
        r = await self._get_redis()

        try:
            raw_data_len = await r.xlen(STREAM_RAW_DATA)
            realtime_len = await r.xlen(STREAM_REALTIME)

            return {
                "raw_data_stream": {
                    "name": STREAM_RAW_DATA,
                    "length": raw_data_len,
                },
                "realtime_stream": {
                    "name": STREAM_REALTIME,
                    "length": realtime_len,
                },
            }
        except redis.RedisError:
            return {"error": "Could not get stream info"}

    async def __aenter__(self) -> "RedisPublisher":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()


# Singleton instance
_publisher: RedisPublisher | None = None


def get_publisher() -> RedisPublisher:
    """Get singleton publisher instance"""
    global _publisher
    if _publisher is None:
        _publisher = RedisPublisher()
    return _publisher
