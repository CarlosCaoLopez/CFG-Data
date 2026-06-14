from redis.asyncio import Redis
from contracts.raw_market_data import MarketDataBatch
from typing import List
import structlog

logger = structlog.get_logger()

# Jobs Stream - A→B: Queue to Workers
class RawDataStream:
    """
    Stream de Redis para datos crudos desde Ingestion.
    Recibe MarketDataBatch y los almacena para que el Scheduler los procese.
    """
    
    def __init__(self, redis_client: Redis, stream_name: str = "raw_data"):
        self.redis = redis_client
        self.stream_name = stream_name
    
    async def add(self, batch: MarketDataBatch) -> str:
        """
        Agrega un MarketDataBatch al stream.

        Args:
            batch: MarketDataBatch con trades y metadata

        Returns:
            message_id: ID del mensaje en Redis (ej: "1234567890123-0")
        """
        message = {
            'payload': batch.model_dump_json(),
            'market_id': batch.metadata.market_id,
            'trades_count': str(len(batch.trades)),
            'ingested_at': batch.ingested_at.isoformat(),
            # Category field for monitoring/filtering in Redis
            'category': batch.metadata.category or 'unknown',
        }

        message_id = await self.redis.xadd(self.stream_name, message)

        logger.info(
            "batch_added_to_stream",
            stream=self.stream_name,
            message_id=message_id,
            market_id=batch.metadata.market_id,
            trades_count=len(batch.trades),
            category=batch.metadata.category,
        )

        return message_id
    
    async def read(
        self, 
        consumer_group: str,
        consumer_name: str,
        count: int = 10,
        block_ms: int = 5000
    ) -> List[tuple[str, MarketDataBatch]]:
        """
        Lee mensajes del stream usando consumer groups.
        
        Args:
            consumer_group: Nombre del grupo de consumidores (ej: "scheduler")
            consumer_name: Nombre de este consumidor (ej: "scheduler-1")
            count: Máximo de mensajes a leer
            block_ms: Tiempo de espera si no hay mensajes (0 = no bloquear)
            
        Returns:
            Lista de tuplas (message_id, MarketDataBatch)
        """
        try:
            # XREADGROUP lee mensajes no procesados del grupo
            response = await self.redis.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={self.stream_name: '>'},
                count=count,
                block=block_ms
            )
            
            if not response:
                return []
            
            # Parsear respuesta de Redis
            results = []
            for stream_name, messages in response:
                for message_id, data in messages:
                    batch = MarketDataBatch.model_validate_json(data[b'payload'])
                    results.append((message_id.decode(), batch))
            
            logger.debug(
                "messages_read_from_stream",
                stream=self.stream_name,
                consumer_group=consumer_group,
                consumer_name=consumer_name,
                messages_count=len(results)
            )
            
            return results
            
        except Exception as e:
            logger.error(
                "error_reading_stream",
                stream=self.stream_name,
                error=str(e)
            )
            raise
    
    async def claim_stale(
        self,
        consumer_group: str,
        consumer_name: str,
        min_idle_ms: int = 600000,
        count: int = 10,
    ) -> List[tuple[str, MarketDataBatch]]:
        """
        Reclama mensajes que llevan pendientes (entregados pero sin ACK) más de
        min_idle_ms y los reasigna a este consumidor para reprocesarlos.

        Necesario porque read() usa XREADGROUP con '>', que solo entrega mensajes
        nuevos: los que quedan huérfanos en la PEL (p.ej. si el orchestrator muere
        a mitad de un batch) nunca se vuelven a leer sin un XCLAIM/XAUTOCLAIM
        explícito. XAUTOCLAIM transfiere la propiedad Y devuelve el cuerpo del
        mensaje, así que el llamante puede procesarlo por la vía normal y ACKearlo.

        Args:
            consumer_group: Nombre del grupo de consumidores
            consumer_name: Consumidor al que se reasignan los mensajes
            min_idle_ms: Idle mínimo para reclamar (default: 10 min). Evita robar
                mensajes que se están procesando ahora mismo.
            count: Máximo de mensajes a reclamar por llamada

        Returns:
            Lista de tuplas (message_id, MarketDataBatch), mismo formato que read().
        """
        try:
            # XAUTOCLAIM stream group consumer min-idle-time start [COUNT n]
            # redis-py devuelve [next_cursor, claimed] o [next_cursor, claimed, deleted]
            result = await self.redis.xautoclaim(
                name=self.stream_name,
                groupname=consumer_group,
                consumername=consumer_name,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count,
            )
        except Exception as e:
            logger.error(
                "error_claiming_stale",
                stream=self.stream_name,
                error=str(e),
            )
            return []

        # Normaliza el retorno de redis-py, que varía entre versiones:
        #   - [(id, fields), ...]                      (solo mensajes)
        #   - (cursor, [(id, fields), ...])            (cursor + mensajes)
        #   - (cursor, [(id, fields), ...], [deleted]) (cursor + mensajes + borrados)
        # El cursor es un ID escalar (str/bytes); los mensajes son 2-tuplas.
        if (
            isinstance(result, (list, tuple))
            and len(result) >= 2
            and isinstance(result[0], (str, bytes))
            and isinstance(result[1], (list, tuple))
        ):
            claimed = result[1]
        else:
            claimed = result or []

        results = []
        for message_id, data in claimed:
            mid = message_id.decode() if isinstance(message_id, bytes) else message_id
            try:
                if not data or b'payload' not in data:
                    # Mensaje recortado/borrado del stream pero aún en la PEL:
                    # se ACKea para sacarlo y que no se reclame eternamente.
                    raise ValueError("message has no payload (trimmed/deleted)")
                batch = MarketDataBatch.model_validate_json(data[b'payload'])
                results.append((mid, batch))
            except Exception as e:
                logger.error(
                    "stale_message_undecodable",
                    stream=self.stream_name,
                    message_id=mid,
                    error=str(e),
                )
                await self.redis.xack(self.stream_name, consumer_group, mid)

        if results:
            logger.warning(
                "stale_messages_reclaimed",
                stream=self.stream_name,
                consumer_group=consumer_group,
                consumer_name=consumer_name,
                count=len(results),
                min_idle_ms=min_idle_ms,
            )

        return results

    async def ack(self, consumer_group: str, message_id: str) -> int:
        """
        Marca un mensaje como procesado.
        
        Args:
            consumer_group: Nombre del grupo de consumidores
            message_id: ID del mensaje a confirmar
            
        Returns:
            Número de mensajes confirmados (1 si éxito, 0 si ya estaba confirmado)
        """
        acked = await self.redis.xack(self.stream_name, consumer_group, message_id)
        
        logger.debug(
            "message_acknowledged",
            stream=self.stream_name,
            consumer_group=consumer_group,
            message_id=message_id,
            acked=acked
        )
        
        return acked
    
    async def create_consumer_group(
        self, 
        consumer_group: str,
        start_id: str = "0"
    ) -> bool:
        """
        Crea un consumer group si no existe.
        
        Args:
            consumer_group: Nombre del grupo
            start_id: Desde dónde empezar a leer ("0" = inicio, "$" = solo nuevos)
            
        Returns:
            True si se creó, False si ya existía
        """
        try:
            await self.redis.xgroup_create(
                name=self.stream_name,
                groupname=consumer_group,
                id=start_id,
                mkstream=True  # Crear stream si no existe
            )
            
            logger.info(
                "consumer_group_created",
                stream=self.stream_name,
                consumer_group=consumer_group,
                start_id=start_id
            )
            
            return True
            
        except Exception as e:
            # BUSYGROUP significa que ya existe
            if "BUSYGROUP" in str(e):
                logger.debug(
                    "consumer_group_already_exists",
                    stream=self.stream_name,
                    consumer_group=consumer_group
                )
                return False
            else:
                logger.error(
                    "error_creating_consumer_group",
                    stream=self.stream_name,
                    error=str(e)
                )
                raise
    
    async def get_pending_count(self, consumer_group: str) -> int:
        """
        Obtiene el número de mensajes pendientes de procesar.
        
        Args:
            consumer_group: Nombre del grupo de consumidores
            
        Returns:
            Número de mensajes pendientes
        """
        try:
            # XPENDING retorna info de mensajes no confirmados
            info = await self.redis.xpending(self.stream_name, consumer_group)
            
            # info[0] es el count total
            count = info[0] if info else 0
            
            return count
            
        except Exception as e:
            logger.error(
                "error_getting_pending_count",
                stream=self.stream_name,
                error=str(e)
            )
            return 0
    
    async def get_stream_length(self) -> int:
        """
        Obtiene el número total de mensajes en el stream.
        
        Returns:
            Longitud del stream
        """
        length = await self.redis.xlen(self.stream_name)
        return length