# Publisher - Sends MarketDataBatch to Redis via queue_system
# Implements QueueProducer interface from queue_system

from datetime import datetime, UTC

import redis.asyncio as redis
import structlog

from queue_system.interfaces.producer import QueueProducer
from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.config import QueueConfig
import json

from contracts.raw_market_data import (
    MarketDataBatch,
    RawTrade,
    RawMarketMetadata,
    InsiderTradeData,
)
from ingestion.config import get_settings

logger = structlog.get_logger()

# Topes de seguridad para los streams publicados directamente con xadd.
# Acotan la memoria de Redis (crítico en despliegues con poca RAM, p.ej. Raspberry Pi):
# sin MAXLEN los streams crecen sin límite porque las entradas consumidas-y-ackeadas
# permanecen hasta un XTRIM/XDEL explícito.
#   - closed_markets: cada entrada es un MarketDataBatch completo (cientos de KB).
#   - insider_trades: entradas mucho más pequeñas (trades de una wallet).
CLOSED_MARKETS_MAXLEN = 500
INSIDER_TRADES_MAXLEN = 2000

# Clave Redis donde se persiste el filtro de categorías (CATEGORIAS_DE_INTERES).
# Vive en el volumen durable de Redis, así que sobrevive a down/build/up.
# El .env sólo actúa de semilla inicial cuando esta clave no existe todavía.
CATEGORIES_CONFIG_KEY = "config:categorias_de_interes"

# Claves Redis donde L1 persiste su estado de seguimiento de mercados.
# Viven en el volumen durable de Redis, así que sobreviven a down/build/up:
# tras un redespliegue L1 rehidrata su lista en vez de redescubrir desde cero.
#   - tracked:markets  (hash)  condition_id -> JSON del dict del market
#   - tracked:processed (set)  condition_ids ya cerrados/procesados
TRACKED_MARKETS_KEY = "tracked:markets"
PROCESSED_MARKETS_KEY = "tracked:processed"

# Clave Redis donde L2 persiste su estado de seguimiento de wallets insider
# (mismo patrón que TRACKED_MARKETS_KEY para L1): hash wallet -> JSON con
# {categories_insider, market_ids_insider, last_seen_ts}. Sin esto,
# _monitored_wallets vive solo en memoria y un redeploy apaga el seguimiento
# de todos los insiders ya detectados hasta la próxima re-detección.
MONITORED_WALLETS_KEY = "monitored:wallets"


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

    async def publish_closed_market_batch(self, batch: MarketDataBatch) -> str:
        """
        Publishes a complete closed-market batch to 'closed_markets' stream.
        Used by L1 (MarketCloseListener) when a tracked market closes.

        Args:
            batch: Complete market data with trades, wallet ages, winning_outcome

        Returns:
            message_id: Redis stream message ID
        """
        settings = get_settings()
        redis_client = await self._get_redis()

        message = {
            "payload": batch.model_dump_json(),
            "market_id": batch.metadata.market_id,
            "trades_count": str(len(batch.trades)),
            "wallets_count": str(len(batch.wallet_ages)),
            "category": batch.metadata.category or "unknown",
            "winning_outcome": batch.metadata.winning_outcome or "unknown",
            "ingested_at": batch.ingested_at.isoformat(),
        }

        try:
            msg_id = await redis_client.xadd(
                settings.closed_markets_stream,
                message,
                maxlen=CLOSED_MARKETS_MAXLEN,
                approximate=True,
            )
            message_id = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

            self.logger.info(
                "closed_market_batch_published",
                message_id=message_id,
                stream=settings.closed_markets_stream,
                market_id=batch.metadata.market_id[:20] + "...",
                trades_count=len(batch.trades),
                wallets_count=len(batch.wallet_ages),
                winning_outcome=batch.metadata.winning_outcome,
            )

            return message_id

        except Exception as e:
            self.logger.error(
                "closed_market_batch_publish_failed",
                market_id=batch.metadata.market_id[:20] + "...",
                error=str(e),
            )
            raise

    async def publish_insider_trades(
        self,
        wallet: str,
        trades: list[RawTrade],
        categories_insider: list[str],
    ) -> str:
        """
        Publishes insider wallet trades to 'insider_trades' stream.
        Used by L2 (InsiderTradesListener) when new trades are found.

        Args:
            wallet: Insider wallet address
            trades: Filtered trades for this wallet
            categories_insider: Categories in which this wallet is insider

        Returns:
            message_id: Redis stream message ID
        """
        settings = get_settings()
        redis_client = await self._get_redis()

        insider_data = InsiderTradeData(
            wallet=wallet,
            trades=trades,
            categories_insider=categories_insider,
        )

        message = {
            "payload": insider_data.model_dump_json(),
            "wallet": wallet,
            "trades_count": str(len(trades)),
            "categories": json.dumps(categories_insider),
            "published_at": insider_data.published_at.isoformat(),
        }

        try:
            msg_id = await redis_client.xadd(
                settings.insider_trades_stream,
                message,
                maxlen=INSIDER_TRADES_MAXLEN,
                approximate=True,
            )
            message_id = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)

            self.logger.info(
                "insider_trades_published",
                message_id=message_id,
                stream=settings.insider_trades_stream,
                wallet=wallet[:10] + "...",
                trades_count=len(trades),
                categories=categories_insider,
            )

            return message_id

        except Exception as e:
            self.logger.error(
                "insider_trades_publish_failed",
                wallet=wallet[:10] + "...",
                error=str(e),
            )
            raise

    async def get_categories_config(self) -> list[str] | None:
        """
        Read the persisted category filter from Redis.

        Returns:
            The list of categories if the key exists, else None (never set).
        """
        redis_client = await self._get_redis()
        raw = await redis_client.get(CATEGORIES_CONFIG_KEY)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        try:
            cats = json.loads(raw)
            if isinstance(cats, list):
                return [str(c) for c in cats]
        except (json.JSONDecodeError, TypeError):
            self.logger.warning("categories_config_parse_failed", raw=raw)
        return None

    async def set_categories_config(self, categories: list[str]) -> None:
        """
        Persist the category filter to Redis so it survives container restarts.

        Args:
            categories: List of categories/tags to monitor.
        """
        redis_client = await self._get_redis()
        await redis_client.set(CATEGORIES_CONFIG_KEY, json.dumps(categories))

    # ================================================================
    # PERSISTENCIA DE MERCADOS EN SEGUIMIENTO (L1)
    # ================================================================
    # Espejo en Redis de los dicts/sets en memoria de MarketCloseListener.
    # Todas las escrituras son defensivas: si Redis falla, se loguea un
    # warning y el sistema sigue operando en memoria (la persistencia es
    # best-effort, no bloquea la ingesta).

    async def load_tracked_markets(self) -> dict[str, dict]:
        """
        Rehidrata la lista de mercados en seguimiento desde Redis.

        Returns:
            dict condition_id -> dict del market. {} si vacío o error.
        """
        try:
            redis_client = await self._get_redis()
            raw_map = await redis_client.hgetall(TRACKED_MARKETS_KEY)
        except Exception as e:
            self.logger.warning("load_tracked_markets_failed", error=str(e))
            return {}

        result: dict[str, dict] = {}
        for field, value in raw_map.items():
            if isinstance(field, bytes):
                field = field.decode()
            if isinstance(value, bytes):
                value = value.decode()
            try:
                result[field] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                self.logger.warning("tracked_market_parse_failed", condition_id=field)
        return result

    async def save_tracked_market(self, condition_id: str, market: dict) -> None:
        """Persiste (o actualiza) un market en seguimiento en Redis."""
        try:
            redis_client = await self._get_redis()
            await redis_client.hset(
                TRACKED_MARKETS_KEY, condition_id, json.dumps(market, default=str)
            )
        except Exception as e:
            self.logger.warning(
                "save_tracked_market_failed", condition_id=condition_id, error=str(e)
            )

    async def remove_tracked_market(self, condition_id: str) -> None:
        """Elimina un market de la lista de seguimiento persistida."""
        try:
            redis_client = await self._get_redis()
            await redis_client.hdel(TRACKED_MARKETS_KEY, condition_id)
        except Exception as e:
            self.logger.warning(
                "remove_tracked_market_failed", condition_id=condition_id, error=str(e)
            )

    async def load_processed_markets(self) -> set[str]:
        """
        Rehidrata el set de mercados ya procesados desde Redis.

        Returns:
            set de condition_ids. set() si vacío o error.
        """
        try:
            redis_client = await self._get_redis()
            members = await redis_client.smembers(PROCESSED_MARKETS_KEY)
        except Exception as e:
            self.logger.warning("load_processed_markets_failed", error=str(e))
            return set()

        return {m.decode() if isinstance(m, bytes) else m for m in members}

    async def add_processed_market(self, condition_id: str) -> None:
        """Marca un market como procesado en Redis (idempotente)."""
        try:
            redis_client = await self._get_redis()
            await redis_client.sadd(PROCESSED_MARKETS_KEY, condition_id)
        except Exception as e:
            self.logger.warning(
                "add_processed_market_failed", condition_id=condition_id, error=str(e)
            )

    # ================================================================
    # Espejo en Redis del dict _monitored_wallets de InsiderTradesListener
    # (L2). Mismo patrón defensivo que el bloque de tracked markets arriba:
    # si Redis falla, se loguea un warning y se sigue en memoria.

    async def load_monitored_wallets(self) -> dict[str, dict]:
        """
        Rehidrata la lista de wallets insider monitorizadas desde Redis.

        Returns:
            dict wallet -> {categories_insider, market_ids_insider, last_seen_ts}.
            {} si vacío o error.
        """
        try:
            redis_client = await self._get_redis()
            raw_map = await redis_client.hgetall(MONITORED_WALLETS_KEY)
        except Exception as e:
            self.logger.warning("load_monitored_wallets_failed", error=str(e))
            return {}

        result: dict[str, dict] = {}
        for field, value in raw_map.items():
            if isinstance(field, bytes):
                field = field.decode()
            if isinstance(value, bytes):
                value = value.decode()
            try:
                result[field] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                self.logger.warning("monitored_wallet_parse_failed", wallet=field)
        return result

    async def save_monitored_wallet(self, wallet: str, data: dict) -> None:
        """Persiste (o actualiza) una wallet monitorizada en Redis."""
        try:
            redis_client = await self._get_redis()
            await redis_client.hset(
                MONITORED_WALLETS_KEY, wallet, json.dumps(data, default=str)
            )
        except Exception as e:
            self.logger.warning(
                "save_monitored_wallet_failed", wallet=wallet, error=str(e)
            )

    async def remove_monitored_wallet(self, wallet: str) -> None:
        """Elimina una wallet de la lista de seguimiento persistida."""
        try:
            redis_client = await self._get_redis()
            await redis_client.hdel(MONITORED_WALLETS_KEY, wallet)
        except Exception as e:
            self.logger.warning(
                "remove_monitored_wallet_failed", wallet=wallet, error=str(e)
            )

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
