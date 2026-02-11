from redis.asyncio import Redis
from contracts.raw_market_data import InsiderTradeData
from typing import List
import structlog

logger = structlog.get_logger()


class InsiderTradesStream:
    """
    Stream de Redis para InsiderTradeData publicado por L2 (InsiderTradesListener).
    Consumidor: QueueOrchestrator (T1 inline), consumer group 'tasks'.
    """

    def __init__(self, redis_client: Redis, stream_name: str = "insider_trades"):
        self.redis = redis_client
        self.stream_name = stream_name

    async def read(
        self,
        consumer_group: str,
        consumer_name: str,
        count: int = 10,
        block_ms: int = 5000,
    ) -> List[tuple[str, InsiderTradeData]]:
        """
        Lee InsiderTradeData del stream usando consumer groups.

        Returns:
            Lista de tuplas (message_id, InsiderTradeData)
        """
        try:
            response = await self.redis.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={self.stream_name: '>'},
                count=count,
                block=block_ms,
            )
            if not response:
                return []
            results = []
            for _, messages in response:
                for message_id, data in messages:
                    trade_data = InsiderTradeData.model_validate_json(data[b'payload'])
                    results.append((message_id.decode(), trade_data))
            logger.debug(
                "insider_trades_read",
                stream=self.stream_name,
                consumer_group=consumer_group,
                count=len(results),
            )
            return results
        except Exception as e:
            logger.error(
                "error_reading_insider_trades_stream",
                stream=self.stream_name,
                error=str(e),
            )
            raise

    async def ack(self, consumer_group: str, message_id: str) -> int:
        """Marca un mensaje como procesado."""
        acked = await self.redis.xack(self.stream_name, consumer_group, message_id)
        logger.debug(
            "insider_trade_acknowledged",
            stream=self.stream_name,
            message_id=message_id,
        )
        return acked

    async def create_consumer_group(
        self, consumer_group: str, start_id: str = "0"
    ) -> bool:
        """Crea un consumer group si no existe (idempotente)."""
        try:
            await self.redis.xgroup_create(
                name=self.stream_name,
                groupname=consumer_group,
                id=start_id,
                mkstream=True,
            )
            logger.info(
                "consumer_group_created",
                stream=self.stream_name,
                consumer_group=consumer_group,
            )
            return True
        except Exception as e:
            if "BUSYGROUP" in str(e):
                logger.debug(
                    "consumer_group_already_exists",
                    stream=self.stream_name,
                    consumer_group=consumer_group,
                )
                return False
            raise

    async def get_stream_length(self) -> int:
        """Longitud total del stream."""
        return await self.redis.xlen(self.stream_name)
