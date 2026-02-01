# Concrete implementation of QueueProducer using Redis Streams
# Used by Persona A (Ingestion) to publish data to the queue

from datetime import datetime, UTC
from threading import Lock
from abc import ABCMeta
from queue_system.interfaces.producer import QueueProducer
from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.exceptions import QueueConnectionError
from contracts.raw_market_data import MarketDataBatch, RawTrade, RawMarketMetadata
from pydantic import ValidationError
from queue_system.config import QueueConfig
import structlog

logger = structlog.get_logger()


class SingletonABCMeta(ABCMeta):
    """
    Combined metaclass that supports both ABC and Singleton patterns.
    Resolves metaclass conflict between ABC and Singleton.
    """

    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        """
        Thread-safe singleton implementation.
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RedisProducer(QueueProducer, metaclass=SingletonABCMeta):
    """
    Concrete implementation of QueueProducer interface.
    Wraps RawDataStream to publish market data batches.
    """

    _instance = None
    _initialized = False

    def __init__(self, raw_data_stream: RawDataStream):
        """
        Initialize producer with a RawDataStream instance.

        Args:
            raw_data_stream: RawDataStream instance connected to Redis
        """
        if not self._initialized:
            self.raw_data_stream = raw_data_stream
            self.logger = logger.bind(component="redis_producer")
            RedisProducer._initialized = True

    @classmethod
    async def get_instance(cls, redis_client, config: QueueConfig):
        """
        Get or create the singleton instance of RedisProducer.

        Args:
            redis_client: Redis client instance
            config: Queue configuration

        Returns:
            RedisProducer singleton instance
        """
        if cls._instance is None:
            raw_data_stream = RawDataStream(redis_client, config)
            cls._instance = cls(raw_data_stream)
        return cls._instance

    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """
        Publishes a complete batch of market data (historical backfill).

        Args:
            batch: Complete market data including metadata, trades, and wallet ages

        Returns:
            batch_id: Unique identifier for the published batch (Redis message ID)

        Raises:
            QueueConnectionError: If Redis connection fails
            ValidationError: If batch data is invalid
        """
        try:
            # Validate batch before publishing
            if not isinstance(batch, MarketDataBatch):
                raise ValidationError("batch must be a MarketDataBatch instance")

            # Publish to raw_data stream
            message_id = await self.raw_data_stream.add(batch)

            self.logger.info(
                "batch_published",
                batch_id=message_id,
                market_id=batch.metadata.market_id,
                trades_count=len(batch.trades),
                wallets_count=len(batch.wallet_ages)
            )

            return message_id

        except ValidationError as e:
            self.logger.error(
                "batch_validation_failed",
                error=str(e),
                market_id=getattr(batch.metadata, 'market_id', 'unknown')
            )
            raise

        except Exception as e:
            self.logger.error(
                "batch_publish_failed",
                error=str(e),
                market_id=getattr(batch.metadata, 'market_id', 'unknown'),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to publish batch: {str(e)}") from e

    async def publish_realtime_trade(
        self,
        trade: RawTrade,
        market: RawMarketMetadata
    ) -> str:
        """
        Publishes a single trade in real-time (high priority).
        Creates a MarketDataBatch with a single trade for immediate processing.

        Args:
            trade: Individual trade data
            market: Market metadata for the trade

        Returns:
            job_id: Unique identifier for the created job (Redis message ID)

        Raises:
            QueueConnectionError: If Redis connection fails
            ValidationError: If trade data is invalid
        """
        try:
            # Validate inputs
            if not isinstance(trade, RawTrade):
                raise ValidationError("trade must be a RawTrade instance")
            if not isinstance(market, RawMarketMetadata):
                raise ValidationError("market must be a RawMarketMetadata instance")

            # Create a single-trade batch for realtime processing
            # Note: wallet_age will be empty for realtime trades,
            # workers should handle this gracefully
            batch = MarketDataBatch(
                metadata=market,
                trades=[trade],
                wallet_ages={},  # Will be enriched by workers if needed
                ingested_at=datetime.now(UTC)
            )

            # Publish using the same mechanism
            message_id = await self.publish_market_batch(batch)

            self.logger.info(
                "realtime_trade_published",
                job_id=message_id,
                tx_hash=trade.tx_hash,
                wallet=trade.wallet,
                market_id=market.market_id,
                size_usdc=trade.size_usdc
            )

            return message_id

        except ValidationError as e:
            self.logger.error(
                "realtime_trade_validation_failed",
                error=str(e),
                tx_hash=getattr(trade, 'tx_hash', 'unknown')
            )
            raise

        except Exception as e:
            self.logger.error(
                "realtime_trade_publish_failed",
                error=str(e),
                tx_hash=getattr(trade, 'tx_hash', 'unknown'),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to publish realtime trade: {str(e)}") from e

    async def health_check(self) -> bool:
        """
        Verifies connection with Redis queue system.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            # Test Redis connection with PING
            await self.raw_data_stream.redis.ping()

            # Verify stream exists by checking length (doesn't fail if empty)
            await self.raw_data_stream.get_stream_length()

            self.logger.debug("health_check_passed")
            return True

        except Exception as e:
            self.logger.warning(
                "health_check_failed",
                error=str(e)
            )
            return False
