# Queue Orchestrator - Main loop for Persona B
# Reads from raw_data + closed_markets + insider_trades.
# T1 inline: persists MarketDataBatch (market + wallets + trades) to BD, then dispatches T2.
# T1 inline: persists InsiderTradeData (insider trades) to BD.

import asyncio
from datetime import datetime, UTC

from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.streams.jobs_stream import JobsStream
from queue_system.streams.insider_trades_stream import InsiderTradesStream
from queue_system.scheduler.job_creator import JobCreator
from queue_system.config import QueueConfig
from database.connection import async_session
from database.manager import DatabaseManager
from contracts.raw_market_data import RawMarketMetadata, RawWalletAge, RawTrade
import structlog

logger = structlog.get_logger()


class QueueOrchestrator:
    """
    Main orchestration loop for Persona B (Queue & Orchestration).

    Responsibilities (Arquitectura Servilleta):
    1. Reads MarketDataBatch from raw_data + closed_markets (published by Persona A)
    2. Persists each batch to BD: Markets, Trades, Wallets  [T1 inline — stub for Persona C]
    3. Dispatches T2 (compute_variables) via Celery after persisting each closed market
    4. Reads InsiderTradeData from insider_trades (published by L2)
    5. Persists each InsiderTradeData to BD: Insiders_Trades  [T1 inline — stub for Persona C]
    6. Creates consumer groups for all streams on startup

    Usage:
        async with QueueOrchestrator(raw_stream, jobs_stream, creator,
                                     closed_markets_stream=cm_stream,
                                     insider_trades_stream=it_stream) as orch:
            await orch.run()
    """

    def __init__(
        self,
        raw_data_stream: RawDataStream,
        jobs_stream: JobsStream,
        job_creator: JobCreator,
        config: QueueConfig | None = None,
        consumer_group: str = "scheduler",
        consumer_name: str = "scheduler-1",
        closed_markets_stream: RawDataStream | None = None,
        insider_trades_stream: InsiderTradesStream | None = None,
    ):
        """
        Initialize orchestrator with stream connections.

        Args:
            raw_data_stream: Stream for reading MarketDataBatch from backfill
            jobs_stream: Kept for backward compatibility (not used in main flow)
            job_creator: JobCreator instance (kept for backward compatibility)
            config: Optional QueueConfig (uses defaults if None)
            consumer_group: Consumer group name for input streams
            consumer_name: Consumer name for this orchestrator instance
            closed_markets_stream: Stream for reading MarketDataBatch from L1
            insider_trades_stream: Stream for reading InsiderTradeData from L2
        """
        self.raw_data_stream = raw_data_stream
        self.jobs_stream = jobs_stream
        self.closed_markets_stream = closed_markets_stream
        self.insider_trades_stream = insider_trades_stream
        self.job_creator = job_creator
        self.config = config or QueueConfig()
        self.consumer_group = consumer_group
        self.consumer_name = consumer_name

        self.logger = logger.bind(
            component="orchestrator",
            consumer_group=consumer_group,
            consumer_name=consumer_name
        )

        # Lifecycle flags
        self._setup_done = False
        self._shutdown = False

        # Metrics
        self._batches_processed = 0
        self._errors = 0

    async def setup(self) -> None:
        """
        Initialize consumer groups for all streams.
        Must be called before run(). Idempotent.
        """
        if self._setup_done:
            self.logger.debug("orchestrator_setup_already_done")
            return

        try:
            # Consumer group for raw_data_stream (backfill batches)
            await self.raw_data_stream.create_consumer_group(
                consumer_group=self.consumer_group,
                start_id="0"
            )

            # Consumer group for closed_markets_stream (L1 realtime closures)
            if self.closed_markets_stream:
                await self.closed_markets_stream.create_consumer_group(
                    consumer_group=self.consumer_group,
                    start_id="0"
                )

            # Consumer group for insider_trades (T1 reads here)
            if self.insider_trades_stream:
                await self.insider_trades_stream.create_consumer_group(
                    consumer_group=self.config.insider_trades_consumer_group,
                    start_id="0"
                )
            else:
                # Ensure group exists even if stream instance not provided
                await self._ensure_consumer_group(
                    stream_name=self.config.insider_trades_stream_name,
                    group_name=self.config.insider_trades_consumer_group,
                )

            # Consumer group for jobs_stream (kept for backward compatibility)
            await self.jobs_stream.create_consumer_group()

            self._setup_done = True

            self.logger.info(
                "orchestrator_setup_complete",
                raw_stream=self.raw_data_stream.stream_name,
                closed_markets_stream=(
                    self.closed_markets_stream.stream_name
                    if self.closed_markets_stream else "not_configured"
                ),
                insider_trades_stream=(
                    self.insider_trades_stream.stream_name
                    if self.insider_trades_stream else "not_configured"
                ),
            )

        except Exception as e:
            self.logger.error(
                "orchestrator_setup_failed",
                error=str(e),
                exc_info=True
            )
            raise

    async def _ensure_consumer_group(
        self, stream_name: str, group_name: str
    ) -> None:
        """Create a consumer group on any stream (idempotent)."""
        redis = self.raw_data_stream.redis
        try:
            await redis.xgroup_create(
                name=stream_name,
                groupname=group_name,
                id="0",
                mkstream=True,
            )
            self.logger.info(
                "consumer_group_created",
                stream=stream_name,
                group=group_name,
            )
        except Exception as e:
            if "BUSYGROUP" in str(e):
                self.logger.debug(
                    "consumer_group_already_exists",
                    stream=stream_name,
                    group=group_name,
                )
            else:
                raise

    async def run(self) -> None:
        """
        Main orchestration loop.
        Runs two concurrent loops:
          - _run_market_batches_loop: closed_markets + raw_data → BD + T2
          - _run_insider_trades_loop: insider_trades → BD
        """
        self.logger.info("orchestrator_started")
        await asyncio.gather(
            self._run_market_batches_loop(),
            self._run_insider_trades_loop(),
        )
        self.logger.info(
            "orchestrator_stopped",
            batches_processed=self._batches_processed,
            errors=self._errors
        )

    async def _run_market_batches_loop(self) -> None:
        """
        Reads MarketDataBatch from closed_markets + raw_data.
        For each batch: persists to BD (stub) then dispatches T2.
        """
        batch_size = self.config.orchestrator_batch_size

        while not self._shutdown:
            try:
                all_batches = await self._read_all_streams(batch_size)

                if not all_batches:
                    continue

                for source_stream, message_id, batch in all_batches:
                    try:
                        await self._process_batch(message_id, batch, source_stream)
                    except Exception as e:
                        self.logger.error(
                            "batch_processing_failed",
                            message_id=message_id,
                            market_id=batch.metadata.market_id,
                            source_stream=source_stream.stream_name,
                            error=str(e),
                            exc_info=True
                        )
                        self._errors += 1
                        # ACK anyway to avoid blocking the queue
                        await source_stream.ack(
                            consumer_group=self.consumer_group,
                            message_id=message_id
                        )

            except Exception as e:
                self.logger.error(
                    "orchestrator_loop_error",
                    error=str(e),
                    exc_info=True
                )
                self._errors += 1
                await asyncio.sleep(5)

    async def _run_insider_trades_loop(self) -> None:
        """
        Reads InsiderTradeData from insider_trades stream.
        Persists each item to BD: Insiders_Trades (stub for Persona C).
        """
        if not self.insider_trades_stream:
            self.logger.info("insider_trades_loop_disabled")
            return

        while not self._shutdown:
            try:
                items = await self.insider_trades_stream.read(
                    consumer_group=self.config.insider_trades_consumer_group,
                    consumer_name=self.consumer_name,
                    count=10,
                    block_ms=5000,
                )
                for message_id, data in items:
                    try:
                        await self._process_insider_trade(message_id, data)
                    except Exception as e:
                        self.logger.error(
                            "insider_trade_processing_failed",
                            message_id=message_id,
                            wallet=data.wallet,
                            error=str(e),
                            exc_info=True
                        )
                        self._errors += 1
                        await self.insider_trades_stream.ack(
                            self.config.insider_trades_consumer_group, message_id
                        )

            except Exception as e:
                self.logger.error(
                    "insider_trades_loop_error",
                    error=str(e),
                    exc_info=True
                )
                self._errors += 1
                await asyncio.sleep(5)

    async def _read_all_streams(
        self, batch_size: int
    ) -> list[tuple[RawDataStream, str, 'MarketDataBatch']]:
        """
        Read from all market input streams (raw_data + closed_markets) in parallel.
        Returns list of (source_stream, message_id, batch) tuples.
        """
        input_streams = [self.raw_data_stream]
        if self.closed_markets_stream:
            input_streams.append(self.closed_markets_stream)

        block_ms = 5000 // len(input_streams)

        read_tasks = [
            stream.read(
                consumer_group=self.consumer_group,
                consumer_name=self.consumer_name,
                count=batch_size,
                block_ms=block_ms,
            )
            for stream in input_streams
        ]

        results = await asyncio.gather(*read_tasks, return_exceptions=True)

        all_batches = []
        for stream, result in zip(input_streams, results):
            if isinstance(result, Exception):
                self.logger.error(
                    "stream_read_error",
                    stream=stream.stream_name,
                    error=str(result),
                )
                self._errors += 1
                continue
            if result:
                all_batches.extend(
                    (stream, msg_id, batch) for msg_id, batch in result
                )

        return all_batches

    # ================================================================
    # CONTRACT → DB DICT CONVERTERS
    # ================================================================

    @staticmethod
    def _market_to_dict(meta: RawMarketMetadata) -> dict:
        return {
            "market_id":      meta.market_id,
            "question":       meta.question,
            "end_date":       meta.end_date,
            "closed":         meta.closed,
            "outcome_prices": meta.outcome_prices,
            "volume_usdc":    meta.volume_usdc,
            "categories":     [meta.category] if meta.category else [],
            "tags":           meta.tags or [],
            "winning_outcome": meta.winning_outcome,
            "updated_at":     datetime.now(UTC),
        }

    @staticmethod
    def _wallet_to_dict(age: RawWalletAge) -> dict:
        return {
            "address":            age.wallet,
            "first_seen_at":      age.first_tx_polymarket or age.first_tx_polygon,
            "first_tx_polymarket": age.first_tx_polymarket,
            "first_tx_polygon":   age.first_tx_polygon,
            "updated_at":         datetime.now(UTC),
        }

    @staticmethod
    def _trade_to_dict(trade: RawTrade) -> dict:
        return {
            "id":          trade.tx_hash,
            "wallet_id":   trade.wallet,
            "market_id":   trade.market_id,
            "timestamp":   trade.timestamp,
            "side":        trade.side,
            "outcome":     trade.outcome,
            "prize":       trade.price,   # contract field is 'price', DB column is 'prize'
            "size_usdc":   trade.size_usdc,
            "size_tokens": trade.size_tokens,
            "last_trade":  trade.last_trade,
            "updated_at":  datetime.now(UTC),
        }

    # ================================================================
    # T1 PROCESSING
    # ================================================================

    async def _process_batch(
        self, message_id: str, batch, source_stream: RawDataStream
    ) -> None:
        """
        T1 inline: persist MarketDataBatch to BD, then dispatch T2.

        FK order: wallets first → market → trades (respects FK constraints).
        Trades are inserted as a single batch statement (ON CONFLICT DO NOTHING).
        """
        from workers.celery_app import app as celery_app

        async with async_session() as session:
            db = DatabaseManager(session)

            # 1. Upsert wallets (small n, ~20-50 per batch; loop is acceptable)
            for age in batch.wallet_ages.values():
                await db.upsert_wallet(self._wallet_to_dict(age))

            # 2. Upsert market
            await db.upsert_market(self._market_to_dict(batch.metadata))

            # 3. Upsert trades in a single batch statement
            if batch.trades:
                await db.upsert_trade_batch(
                    [self._trade_to_dict(t) for t in batch.trades]
                )

            await session.commit()

        self.logger.info(
            "t1_persisted",
            market_id=batch.metadata.market_id,
            trades=len(batch.trades),
            wallets=len(batch.wallet_ages),
            source=source_stream.stream_name,
        )

        # Dispatch T2: compute SQL variables for this closed market
        celery_app.send_task(
            'workers.tasks.compute_variables',
            args=[batch.metadata.market_id],
        )
        self.logger.info(
            "t2_dispatched",
            market_id=batch.metadata.market_id,
        )

        # ACK the batch in its source stream
        await source_stream.ack(
            consumer_group=self.consumer_group,
            message_id=message_id
        )

        self._batches_processed += 1

        self.logger.info(
            "batch_processed",
            message_id=message_id,
            market_id=batch.metadata.market_id,
            source_stream=source_stream.stream_name,
            trades_in_batch=len(batch.trades),
        )

    async def _process_insider_trade(self, message_id: str, data) -> None:
        """
        T1 inline for insider_trades: persist InsiderTradeData to BD.
        Inserts all trades for the wallet as a single batch statement.
        """
        if data.trades:
            async with async_session() as session:
                db = DatabaseManager(session)
                await db.upsert_insider_trade_batch(
                    [self._trade_to_dict(t) for t in data.trades]
                )
                await session.commit()

        self.logger.info(
            "t1_insider_persisted",
            wallet=data.wallet,
            trades=len(data.trades),
            categories=data.categories_insider,
        )

        await self.insider_trades_stream.ack(
            self.config.insider_trades_consumer_group, message_id
        )

        self.logger.info(
            "insider_trade_processed",
            message_id=message_id,
            wallet=data.wallet,
            trades=len(data.trades),
        )

    def shutdown(self) -> None:
        """Signal the orchestrator to stop gracefully."""
        self.logger.info("orchestrator_shutdown_requested")
        self._shutdown = True

    async def cleanup(self) -> None:
        """Clean up orchestrator resources (resets internal state)."""
        self.logger.info(
            "orchestrator_cleanup_started",
            batches_processed=self._batches_processed,
            errors=self._errors
        )
        self._setup_done = False
        self._shutdown = False
        self.logger.info("orchestrator_cleanup_complete")

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
        if not self._shutdown:
            self.shutdown()
        await self.cleanup()
        return False

    async def get_metrics(self) -> dict:
        """Returns current orchestrator metrics."""
        try:
            pending_raw = await self.raw_data_stream.get_pending_count(
                consumer_group=self.consumer_group
            )

            stream_lengths = {
                'raw_data': await self.raw_data_stream.get_stream_length(),
            }

            pending_closed = 0
            if self.closed_markets_stream:
                pending_closed = await self.closed_markets_stream.get_pending_count(
                    consumer_group=self.consumer_group
                )
                stream_lengths['closed_markets'] = (
                    await self.closed_markets_stream.get_stream_length()
                )

            if self.insider_trades_stream:
                stream_lengths['insider_trades'] = (
                    await self.insider_trades_stream.get_stream_length()
                )

            return {
                'batches_processed': self._batches_processed,
                'errors': self._errors,
                'pending_batches_raw': pending_raw,
                'pending_batches_closed': pending_closed,
                'stream_lengths': stream_lengths,
            }
        except Exception as e:
            self.logger.error("get_metrics_failed", error=str(e), exc_info=True)
            return {
                'batches_processed': self._batches_processed,
                'errors': self._errors,
                'error': str(e),
            }
