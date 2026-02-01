# Concrete implementation of QueueConsumer using Redis Streams
# Used by Persona C (Workers) to consume processing jobs

import asyncio
from queue_system.interfaces.consumer import QueueConsumer
from queue_system.streams.jobs_stream import JobsStream
from queue_system.exceptions import QueueConnectionError, JobNotFoundError
from queue_system.config import QueueConfig
from queue_system.reliability.retry_handler import RetryHandler
from queue_system.reliability.dead_letter import DeadLetterQueue
from contracts.processing_job import ProcessingJob, JobResult
import structlog

logger = structlog.get_logger()


class RedisConsumer(QueueConsumer):
    """
    Concrete implementation of QueueConsumer interface.
    Wraps JobsStream to consume and process jobs with retry logic.
    """

    def __init__(
        self,
        jobs_stream: JobsStream,
        retry_handler: RetryHandler,
        dead_letter_queue: DeadLetterQueue,
        config: QueueConfig | None = None
    ):
        """
        Initialize consumer with a JobsStream instance.

        Args:
            jobs_stream: JobsStream instance connected to Redis
            retry_handler: RetryHandler for exponential backoff logic
            dead_letter_queue: DeadLetterQueue for permanently failed jobs
            config: Optional QueueConfig for retry parameters (uses defaults if None)
        """
        self.jobs_stream = jobs_stream
        self.retry_handler = retry_handler
        self.dead_letter_queue = dead_letter_queue
        self.config = config or QueueConfig()
        self.logger = logger.bind(component="redis_consumer")

        # Internal tracking: job_id -> Redis message_id
        self._message_ids: dict[str, str] = {}

        # Cache full jobs for retry logic: job_id -> ProcessingJob
        self._jobs_cache: dict[str, ProcessingJob] = {}

    async def get_next_job(
        self,
        worker_id: str,
        timeout_ms: int = 5000
    ) -> ProcessingJob | None:
        """
        Retrieves the next job from the queue, respecting FIFO order.
        Blocks until a job is available or timeout is reached.

        Args:
            worker_id: Unique identifier for this worker
            timeout_ms: Maximum time to wait for a job in milliseconds

        Returns:
            ProcessingJob if available, None if timeout reached

        Raises:
            QueueConnectionError: If Redis connection fails
        """
        try:
            # Read next job from stream
            result = await self.jobs_stream.read_next_job(worker_id, timeout_ms)

            if result is None:
                return None

            job, message_id = result

            # Cache message_id and job for later ack/nack
            self._message_ids[job.job_id] = message_id
            self._jobs_cache[job.job_id] = job

            self.logger.debug(
                "job_retrieved",
                worker_id=worker_id,
                job_id=job.job_id,
                job_type=job.job_type.value,
                retry_count=job.retry_count
            )

            return job

        except Exception as e:
            self.logger.error(
                "get_next_job_failed",
                worker_id=worker_id,
                error=str(e),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to get next job: {str(e)}") from e

    async def ack_job(self, job_id: str, result: JobResult) -> None:
        """
        Acknowledges successful job completion.
        Removes job from pending list and cleans up cache.

        Args:
            job_id: ID of the completed job
            result: Processing result with metrics

        Raises:
            QueueConnectionError: If Redis connection fails
            JobNotFoundError: If job_id doesn't exist in cache
        """
        try:
            # Retrieve message_id from cache
            message_id = self._message_ids.get(job_id)
            if message_id is None:
                raise JobNotFoundError(f"Job {job_id} not found in message tracking")

            # ACK in Redis stream
            await self.jobs_stream.ack_job(message_id)

            # Clean up caches
            del self._message_ids[job_id]
            self._jobs_cache.pop(job_id, None)

            self.logger.info(
                "job_acknowledged",
                job_id=job_id,
                success=result.success,
                scores_calculated=result.scores_calculated,
                alerts_generated=result.alerts_generated,
                processing_time_ms=result.processing_time_ms
            )

        except JobNotFoundError:
            raise

        except Exception as e:
            self.logger.error(
                "ack_job_failed",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to acknowledge job: {str(e)}") from e

    async def nack_job(self, job_id: str, error: str, retry: bool = True) -> None:
        """
        Rejects a job due to processing failure.
        Implements retry logic with exponential backoff.

        Args:
            job_id: ID of the failed job
            error: Error message describing the failure
            retry: If True, re-queue the job for retry. If False, send to dead letter queue

        Raises:
            QueueConnectionError: If Redis connection fails
            JobNotFoundError: If job_id doesn't exist in cache
        """
        try:
            # Retrieve message_id and job from cache
            message_id = self._message_ids.get(job_id)
            if message_id is None:
                raise JobNotFoundError(f"Job {job_id} not found in message tracking")

            job = self._jobs_cache.get(job_id)
            if job is None:
                raise JobNotFoundError(f"Job {job_id} not found in jobs cache")

            # Decide whether to retry based on retry_count using RetryHandler
            should_retry = retry and self.retry_handler.should_retry(job)

            if should_retry:
                # Calculate exponential backoff delay
                backoff_delay = self.retry_handler.calculate_backoff_delay(job.retry_count)

                # Get retry metadata for logging
                retry_metadata = self.retry_handler.get_retry_metadata(
                    job=job,
                    error=error,
                    next_retry_delay=backoff_delay
                )

                self.logger.warning(
                    "job_requeued_with_backoff",
                    **retry_metadata,
                    backoff_delay_seconds=round(backoff_delay, 2)
                )

                # Wait for backoff delay before re-queuing
                await asyncio.sleep(backoff_delay)

                # Re-queue with incremented retry_count
                await self.jobs_stream.nack_job(
                    message_id=message_id,
                    job=job,
                    error=error,
                    requeue=True
                )
            else:
                # Exceeded max retries or retry=False, send to Dead Letter Queue
                await self.jobs_stream.nack_job(
                    message_id=message_id,
                    job=job,
                    error=error,
                    requeue=False
                )

                # Send to DLQ for manual review
                dlq_message_id = await self.dead_letter_queue.send_to_dlq(
                    job=job,
                    error=error,
                    original_message_id=message_id,
                    original_stream=self.config.jobs_stream_name
                )

                self.logger.error(
                    "job_failed_permanently_sent_to_dlq",
                    job_id=job_id,
                    job_type=job.job_type.value,
                    retry_count=job.retry_count,
                    max_retries=self.config.max_retries,
                    error=error,
                    dlq_message_id=dlq_message_id
                )

            # Clean up caches
            del self._message_ids[job_id]
            self._jobs_cache.pop(job_id, None)

        except JobNotFoundError:
            raise

        except Exception as e:
            self.logger.error(
                "nack_job_failed",
                job_id=job_id,
                error=str(e),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to nack job: {str(e)}") from e

    async def get_pending_count(self) -> int:
        """
        Returns the number of pending jobs in the queue.
        Useful for monitoring and load balancing.

        Returns:
            Number of jobs waiting to be processed

        Raises:
            QueueConnectionError: If Redis connection fails
        """
        try:
            count = await self.jobs_stream.get_pending_count()

            self.logger.debug(
                "pending_count_retrieved",
                pending_count=count
            )

            return count

        except Exception as e:
            self.logger.error(
                "get_pending_count_failed",
                error=str(e),
                exc_info=True
            )
            raise QueueConnectionError(f"Failed to get pending count: {str(e)}") from e
