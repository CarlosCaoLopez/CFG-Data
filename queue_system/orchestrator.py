# Queue Orchestrator - Main loop for Persona B
# Reads from raw_data_stream, converts batches to jobs, publishes to jobs_stream

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
    1. Reads MarketDataBatch from raw_data_stream (published by Persona A)
    2. Converts batches to ProcessingJobs using JobCreator
    3. Enqueues jobs to jobs_stream (consumed by Persona C)
    4. Handles errors and monitors throughput

    Usage:
        # Recommended: Use as async context manager
        async with QueueOrchestrator(raw_stream, jobs_stream, creator) as orch:
            await orch.run()

        # Alternative: Manual lifecycle management
        orch = QueueOrchestrator(raw_stream, jobs_stream, creator)
        await orch.setup()
        try:
            await orch.run()
        finally:
            orch.shutdown()
            await orch.cleanup()
    """

    def __init__(
        self,
        raw_data_stream: RawDataStream,
        jobs_stream: JobsStream,
        job_creator: JobCreator,
        config: QueueConfig | None = None,
        consumer_group: str = "scheduler",
        consumer_name: str = "scheduler-1"
    ):
        """
        Initialize orchestrator with stream connections.

        Args:
            raw_data_stream: Stream for reading MarketDataBatch from Persona A
            jobs_stream: Stream for publishing ProcessingJobs to Persona C
            job_creator: JobCreator instance for converting batches to jobs
            config: Optional QueueConfig (uses defaults if None)
            consumer_group: Consumer group name for raw_data_stream
            consumer_name: Consumer name for this orchestrator instance
        """
        self.raw_data_stream = raw_data_stream
        self.jobs_stream = jobs_stream
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
        Initialize consumer groups for both streams.
        Must be called before run().

        This method is idempotent - calling it multiple times is safe.
        """
        if self._setup_done:
            self.logger.debug("orchestrator_setup_already_done")
            return

        try:
            # Create consumer group for raw_data_stream
            await self.raw_data_stream.create_consumer_group(
                consumer_group=self.consumer_group,
                start_id="0"  # Process from beginning
            )

            # Create consumer group for jobs_stream
            await self.jobs_stream.create_consumer_group()

            self._setup_done = True

            self.logger.info(
                "orchestrator_setup_complete",
                raw_stream=self.raw_data_stream.stream_name,
                jobs_stream=self.jobs_stream.stream_name
            )

        except Exception as e:
            self.logger.error(
                "orchestrator_setup_failed",
                error=str(e),
                exc_info=True
            )
            raise

    async def run(self) -> None:
        """
        Main orchestration loop.
        Runs continuously until shutdown() is called.
        """
        self.logger.info("orchestrator_started")

        while not self._shutdown:
            try:
                # Read batches from raw_data_stream
                batches = await self.raw_data_stream.read(
                    consumer_group=self.consumer_group,
                    consumer_name=self.consumer_name,
                    count=10,  # Process up to 10 batches per iteration
                    block_ms=5000  # Block for 5 seconds if no data
                )

                if not batches:
                    # No data available, continue polling
                    continue

                # Process each batch
                for message_id, batch in batches:
                    try:
                        await self._process_batch(message_id, batch)
                    except Exception as e:
                        self.logger.error(
                            "batch_processing_failed",
                            message_id=message_id,
                            market_id=batch.metadata.market_id,
                            error=str(e),
                            exc_info=True
                        )
                        self._errors += 1

                        # ACK the batch anyway to avoid blocking the queue
                        # In production, might want to send to dead letter queue
                        await self.raw_data_stream.ack(
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

    async def _process_batch(self, message_id: str, batch) -> None:
        """
        Process a single MarketDataBatch.

        Args:
            message_id: Redis message ID for ACK
            batch: MarketDataBatch to process
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
                # Continue with other jobs even if one fails

        # ACK the batch in raw_data_stream
        await self.raw_data_stream.ack(
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
        Returns current orchestrator metrics.

        Returns:
            Dictionary with metrics:
            - batches_processed: Total batches processed
            - jobs_created: Total jobs created
            - errors: Total errors encountered
            - pending_batches: Pending batches in raw_data_stream
            - pending_jobs: Pending jobs in jobs_stream
        """
        try:
            pending_batches = await self.raw_data_stream.get_pending_count(
                consumer_group=self.consumer_group
            )
            pending_jobs = await self.jobs_stream.get_pending_count()

            return {
                'batches_processed': self._batches_processed,
                'jobs_created': self._jobs_created,
                'errors': self._errors,
                'pending_batches': pending_batches,
                'pending_jobs': pending_jobs,
                'stream_lengths': {
                    'raw_data': await self.raw_data_stream.get_stream_length(),
                    'jobs': await self.jobs_stream.get_stream_length()
                }
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
                'error': str(e)
            }
