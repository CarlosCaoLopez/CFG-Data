# QueueProducer - Abstract interface for Persona A to publish data

from abc import ABC, abstractmethod
from contracts.raw_market_data import MarketDataBatch, RawTrade, RawMarketMetadata


class QueueProducer(ABC):
    """
    Abstract interface for publishing data to the queue system.
    Used by Persona A (Ingestion) to send data to Persona B (Queue/Orchestration).
    """

    @abstractmethod
    async def publish_market_batch(self, batch: MarketDataBatch) -> str:
        """
        Publishes a complete batch of market data (historical backfill).

        Args:
            batch: Complete market data including metadata, trades, and wallet ages

        Returns:
            batch_id: Unique identifier for the published batch

        Raises:
            QueueConnectionError: If Redis connection fails
            ValidationError: If batch data is invalid
        """
        pass

    @abstractmethod
    async def publish_realtime_trade(
        self,
        trade: RawTrade,
        market: RawMarketMetadata
    ) -> str:
        """
        Publishes a single trade in real-time (high priority).
        Used for WebSocket streaming data that needs immediate processing.

        Args:
            trade: Individual trade data
            market: Market metadata for the trade

        Returns:
            job_id: Unique identifier for the created job

        Raises:
            QueueConnectionError: If Redis connection fails
            ValidationError: If trade data is invalid
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Verifies connection with Redis queue system.

        Returns:
            True if connection is healthy, False otherwise
        """
        pass
