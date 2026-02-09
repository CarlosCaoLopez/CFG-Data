# Queue Orchestrator - Main loop for Persona B
# Reads from raw_data + closed_markets, converts batches to jobs, publishes to jobs_stream

import asyncio
from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.streams.jobs_stream import JobsStream
from queue_system.scheduler.job_creator import JobCreator
from queue_system.config import QueueConfig
import structlog

logger = structlog.get_logger()


class QueueOrchestrator:
    """
    Main orchestration loop for Persona B (Queue & Orchestration).

    Responsibilities:
    1. Reads MarketDataBatch from raw_data + closed_markets (published by Persona A)
    2. Converts batches to ProcessingJobs using JobCreator
    3. Enqueues jobs to jobs_stream (consumed by Persona C workers)
    4. Creates consumer groups for all streams (including insider_trades for T1)
    5. Handles errors and monitors throughput

    Usage:
        async with QueueOrchestrator(raw_stream, jobs_stream, creator,
                                     closed_markets_stream=cm_stream) as orch:
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
    ):
        """
        Initialize orchestrator with stream connections.

        Args:
            raw_data_stream: Stream for reading MarketDataBatch from Persona A (backfill)
            jobs_stream: Stream for publishing ProcessingJobs to Persona C
            job_creator: JobCreator instance for converting batches to jobs
            config: Optional QueueConfig (uses defaults if None)
            consumer_group: Consumer group name for input streams
            consumer_name: Consumer name for this orchestrator instance
            closed_markets_stream: Stream for reading MarketDataBatch from L1 (realtime closures)
        """
        self.raw_data_stream = raw_data_stream
        self.jobs_stream = jobs_stream
        self.closed_markets_stream = closed_markets_stream
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
        self._jobs_created = 0
        self._errors = 0

    async def setup(self) -> None:
        """
        Initialize consumer groups for all streams.
        Must be called before run().

        Creates groups for:
        - raw_data (orchestrator reads backfill batches)
        - closed_markets (orchestrator reads L1 closure batches)
        - insider_trades (T1 reads L2 insider data — group created here for readiness)
        - jobs_stream (workers consume processing jobs)

        This method is idempotent - calling it multiple times is safe.
        """
        if self._setup_done:
            self.logger.debug("orchestrator_setup_already_done")
            return

        try:
            # Create consumer group for raw_data_stream (orchestrator reads)
            await self.raw_data_stream.create_consumer_group(
                consumer_group=self.consumer_group,
                start_id="0"
            )

            # Create consumer group for closed_markets_stream (orchestrator reads)
            if self.closed_markets_stream:
                await self.closed_markets_stream.create_consumer_group(
                    consumer_group=self.consumer_group,
                    start_id="0"
                )

            # Create consumer group for insider_trades (T1 consumes, not orchestrator)
            await self._ensure_consumer_group(
                stream_name=self.config.insider_trades_stream_name,
                group_name=self.config.insider_trades_consumer_group,
            )

            # Create consumer group for jobs_stream (workers consume)
            await self.jobs_stream.create_consumer_group()

            self._setup_done = True

            self.logger.info(
                "orchestrator_setup_complete",
                raw_stream=self.raw_data_stream.stream_name,
                closed_markets_stream=(
                    self.closed_markets_stream.stream_name
                    if self.closed_markets_stream else "not_configured"
                ),
                insider_trades_stream=self.config.insider_trades_stream_name,
                jobs_stream=self.jobs_stream.stream_name,
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
        """
        Create a consumer group on any stream (idempotent).
        Uses the Redis client from raw_data_stream.
        """
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
        Reads from raw_data and closed_markets, converts to jobs, publishes.
        Runs continuously until shutdown() is called.
        """
        self.logger.info("orchestrator_started")

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

                        # ACK the batch anyway to avoid blocking the queue
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

                # Backoff on error to avoid tight loop
                await asyncio.sleep(5)

        self.logger.info(
            "orchestrator_stopped",
            batches_processed=self._batches_processed,
            jobs_created=self._jobs_created,
            errors=self._errors
        )

    async def _read_all_streams(
        self, batch_size: int
    ) -> list[tuple[RawDataStream, str, 'MarketDataBatch']]:
        """
        Read from all input streams (raw_data + closed_markets) in parallel.

        Returns list of (source_stream, message_id, batch) tuples.
        """
        input_streams = [self.raw_data_stream]
        if self.closed_markets_stream:
            input_streams.append(self.closed_markets_stream)

        # Split block time across streams so total wait is ~5s
        block_ms = 5000 // len(input_streams)

        # Read from all streams in parallel
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

    async def _process_batch(
        self, message_id: str, batch, source_stream: RawDataStream
    ) -> None:
        """
        Process a single MarketDataBatch from any input stream.

        Args:
            message_id: Redis message ID for ACK
            batch: MarketDataBatch to process
            source_stream: The stream this batch came from (for ACK)
        """
        start_time = asyncio.get_event_loop().time()

        # Convert batch to jobs
        jobs = await self.job_creator.process_batch(batch)

        # Enqueue each job
        jobs_enqueued = 0
        for job in jobs:
            try:
                await self.jobs_stream.add_job(job)
                jobs_enqueued += 1
            except Exception as e:
                self.logger.error(
                    "job_enqueue_failed",
                    job_id=job.job_id,
                    job_type=job.job_type.value,
                    error=str(e)
                )

        # ACK the batch in its source stream
        await source_stream.ack(
            consumer_group=self.consumer_group,
            message_id=message_id
        )

        # Update metrics
        self._batches_processed += 1
        self._jobs_created += jobs_enqueued

        elapsed_ms = int((asyncio.get_event_loop().time() - start_time) * 1000)

        self.logger.info(
            "batch_processed",
            message_id=message_id,
            market_id=batch.metadata.market_id,
            source_stream=source_stream.stream_name,
            trades_in_batch=len(batch.trades),
            jobs_created=len(jobs),
            jobs_enqueued=jobs_enqueued,
            processing_time_ms=elapsed_ms
        )

    def shutdown(self) -> None:
        """
        Signal the orchestrator to stop gracefully.
        The run() loop will complete its current iteration and exit.
        """
        self.logger.info("orchestrator_shutdown_requested")
        self._shutdown = True

    async def cleanup(self) -> None:
        """
        Clean up orchestrator resources.

        Note: Does NOT close Redis connections as they are managed externally.
        Only resets internal state (SOLID principle).
        """
        self.logger.info(
            "orchestrator_cleanup_started",
            batches_processed=self._batches_processed,
            jobs_created=self._jobs_created,
            errors=self._errors
        )

        # Reset lifecycle flags
        self._setup_done = False
        self._shutdown = False

        # Optionally reset metrics (uncomment if needed)
        # self._batches_processed = 0
        # self._jobs_created = 0
        # self._errors = 0

        self.logger.info("orchestrator_cleanup_complete")

    async def __aenter__(self):
        """
        Async context manager entry.
        Automatically calls setup() when entering the context.

        Usage:
            async with QueueOrchestrator(...) as orch:
                await orch.run()
        """
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
        """
        Async context manager exit.
        Automatically calls shutdown() and cleanup() when exiting the context.

        Args:
            exc_type: Exception type if an exception occurred (unused)
            exc_val: Exception value if an exception occurred (unused)
            exc_tb: Exception traceback if an exception occurred (unused)

        Returns:
            False to propagate exceptions (does not suppress them)
        """
        # Signal shutdown if still running
        if not self._shutdown:
            self.shutdown()

        # Clean up resources
        await self.cleanup()

        # Don't suppress exceptions
        return False

    async def get_metrics(self) -> dict:
        """
        Returns current orchestrator metrics including all streams.
        """
        try:
            pending_raw = await self.raw_data_stream.get_pending_count(
                consumer_group=self.consumer_group
            )
            pending_jobs = await self.jobs_stream.get_pending_count()

            stream_lengths = {
                'raw_data': await self.raw_data_stream.get_stream_length(),
                'jobs': await self.jobs_stream.get_stream_length(),
            }

            if self.closed_markets_stream:
                pending_closed = await self.closed_markets_stream.get_pending_count(
                    consumer_group=self.consumer_group
                )
                stream_lengths['closed_markets'] = (
                    await self.closed_markets_stream.get_stream_length()
                )
            else:
                pending_closed = 0

            # insider_trades length (read-only, not consumed by orchestrator)
            try:
                insider_len = await self.raw_data_stream.redis.xlen(
                    self.config.insider_trades_stream_name
                )
                stream_lengths['insider_trades'] = insider_len
            except Exception:
                pass

            return {
                'batches_processed': self._batches_processed,
                'jobs_created': self._jobs_created,
                'errors': self._errors,
                'pending_batches_raw': pending_raw,
                'pending_batches_closed': pending_closed,
                'pending_jobs': pending_jobs,
                'stream_lengths': stream_lengths,
            }
        except Exception as e:
            self.logger.error(
                "get_metrics_failed",
                error=str(e),
                exc_info=True
            )
            return {
                'batches_processed': self._batches_processed,
                'jobs_created': self._jobs_created,
                'errors': self._errors,
                'error': str(e),
            }
