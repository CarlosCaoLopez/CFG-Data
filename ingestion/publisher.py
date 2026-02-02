# Publisher - Sends MarketDataBatch to Redis via queue_system
# Implements QueueProducer interface from queue_system

from datetime import datetime, UTC

import redis.asyncio as redis
import structlog

from queue_system.interfaces.producer import QueueProducer
from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.config import QueueConfig
from contracts.raw_market_data import MarketDataBatch, RawTrade, RawMarketMetadata

logger = structlog.get_logger()


class RedisPublisher(QueueProducer):
    """
    Redis Streams implementation of QueueProducer.

    Uses queue_system's RawDataStream to publish MarketDataBatch
    for processing by Persona B (Orchestrator).
    """

    def __init__(self, config: QueueConfig | None = None):
        self.config = config or QueueConfig()
        self._redis: redis.Redis | None = None
        self._raw_data_stream: RawDataStream | None = None
        self.logger = logger.bind(component="redis_publisher")

    async def _get_redis(self) -> redis.Redis:
        """Lazy Redis connection"""
        if self._redis is None:
            # Note: Do NOT use decode_responses=True
            # queue_system's RawDataStream expects bytes for consistency
            self._redis = redis.from_url(self.config.redis_url)
        return self._redis

    async def _get_stream(self) -> RawDataStream:
        """Lazy RawDataStream initialization"""
        if self._raw_data_stream is None:
            redis_client = await self._get_redis()
            self._raw_data_stream = RawDataStream(
                redis_client,
                self.config.raw_data_stream_name
            )
        return self._raw_data_stream

    async def close(self) -> None:
        """Close Redis connection"""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
            self._raw_data_stream = None

    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """
        Publishes a complete batch of market data to Redis Stream.

        Args:
            batch: Complete market data including metadata, trades, and wallet ages

        Returns:
            batch_id: Redis message ID for the published batch
        """
        stream = await self._get_stream()

        try:
            message_id = await stream.add(batch)

            # Handle bytes response when decode_responses=False
            if isinstance(message_id, bytes):
                message_id = message_id.decode()

            self.logger.info(
                "batch_published",
                batch_id=message_id,
                stream=self.config.raw_data_stream_name,
                market_id=batch.metadata.market_id[:20] + "...",
                trades_count=len(batch.trades),
                wallets_count=len(batch.wallet_ages),
            )

            return message_id

        except Exception as e:
            self.logger.error(
                "batch_publish_failed",
                market_id=batch.metadata.market_id[:20] + "...",
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

        Creates a MarketDataBatch with a single trade for immediate processing.

        Args:
            trade: Individual trade data
            market: Market metadata for the trade

        Returns:
            job_id: Redis message ID for the created job
        """
        # Wrap single trade in a MarketDataBatch
        batch = MarketDataBatch(
            metadata=market,
            trades=[trade],
            wallet_ages={},  # Workers will enrich wallet data if needed
            ingested_at=datetime.now(UTC),
        )

        stream = await self._get_stream()

        try:
            message_id = await stream.add(batch)

            # Handle bytes response when decode_responses=False
            if isinstance(message_id, bytes):
                message_id = message_id.decode()

            self.logger.info(
                "realtime_trade_published",
                job_id=message_id,
                stream=self.config.raw_data_stream_name,
                tx_hash=trade.tx_hash[:20] + "...",
                market_id=market.market_id[:20] + "...",
            )

            return message_id

        except Exception as e:
            self.logger.error(
                "realtime_trade_publish_failed",
                tx_hash=trade.tx_hash[:20] + "...",
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
        except Exception as e:
            self.logger.warning("redis_health_check_failed", error=str(e))
            return False

    async def get_stream_info(self) -> dict:
        """
        Get information about the Redis stream.

        Returns:
            Dict with stream length and info
        """
        try:
            stream = await self._get_stream()
            length = await stream.get_stream_length()

            return {
                "raw_data_stream": {
                    "name": self.config.raw_data_stream_name,
                    "length": length,
                },
            }
        except Exception as e:
            self.logger.error("get_stream_info_failed", error=str(e))
            return {"error": str(e)}

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
