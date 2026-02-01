"""
Redis Stream for distributing ProcessingJobs to workers.
Workers consume jobs in FIFO order using consumer groups.
"""

from redis.asyncio import Redis
from contracts.processing_job import ProcessingJob, JobResult
from datetime import datetime
from typing import Optional, Tuple
import structlog

logger = structlog.get_logger()

# Forward reference to avoid circular imports
# These are imported at runtime when needed
DeadLetterQueue = None
RateLimiter = None


class JobsStream:
    """
    Single Redis Stream for job distribution.
    Workers consume jobs in FIFO order using consumer groups.
    """

    def __init__(
        self,
        redis_client: Redis,
        stream_name: str = "jobs_stream",
        consumer_group: str = "workers",
        dead_letter_queue: Optional['DeadLetterQueue'] = None,
        rate_limiter: Optional['RateLimiter'] = None
    ):
        self.redis = redis_client
        self.stream_name = stream_name
        self.consumer_group = consumer_group
        self.dead_letter_queue = dead_letter_queue
        self.rate_limiter = rate_limiter
        self.logger = logger.bind(stream=stream_name)

    async def add_job(self, job: ProcessingJob) -> str:
        """
        Publishes a ProcessingJob to the stream.
        Applies rate limiting if configured to prevent worker saturation.

        Args:
            job: ProcessingJob to enqueue

        Returns:
            message_id from Redis (e.g., "1234567890-0")
        """
        # Apply rate limiting if rate_limiter is configured
        if self.rate_limiter is not None:
            # Wait for capacity before adding job
            await self.rate_limiter.wait_for_capacity(tokens=1)

        message_data = {
            "job_data": job.model_dump_json(),
            "job_id": job.job_id,
            "job_type": job.job_type.value,
            "created_at": job.created_at.isoformat(),
            "retry_count": str(job.retry_count)
        }

        message_id = await self.redis.xadd(
            self.stream_name,
            message_data
        )

        stream_length = await self.get_stream_length()

        self.logger.info(
            "job_added",
            job_id=job.job_id,
            job_type=job.job_type.value,
            message_id=message_id.decode() if isinstance(message_id, bytes) else message_id,
            retry_count=job.retry_count,
            stream_length=stream_length
        )

        return message_id.decode() if isinstance(message_id, bytes) else message_id

    async def read_next_job(
        self,
        worker_id: str,
        timeout_ms: int = 5000
    ) -> Optional[Tuple[ProcessingJob, str]]:
        """
        Reads the next available job from the stream.

        Args:
            worker_id: Unique worker identifier
            timeout_ms: Maximum wait time in milliseconds

        Returns:
            Tuple (ProcessingJob, message_id) or None if timeout
        """
        try:
            # XREADGROUP BLOCK timeout_ms STREAMS stream_name >
            messages = await self.redis.xreadgroup(
                groupname=self.consumer_group,
                consumername=worker_id,
                streams={self.stream_name: '>'},
                count=1,
                block=timeout_ms
            )

            if not messages:
                return None

            # messages format: [(stream_name, [(message_id, data)])]
            stream_name, message_list = messages[0]
            if not message_list:
                return None

            message_id, data = message_list[0]

            # Deserialize
            job_data = data.get(b'job_data') or data.get('job_data')
            if isinstance(job_data, bytes):
                job_data = job_data.decode()

            job = ProcessingJob.model_validate_json(job_data)

            message_id_str = message_id.decode() if isinstance(message_id, bytes) else message_id

            pending_count = await self.get_pending_count()

            self.logger.info(
                "job_consumed",
                job_id=job.job_id,
                job_type=job.job_type.value,
                worker_id=worker_id,
                message_id=message_id_str,
                retry_count=job.retry_count,
                pending_count=pending_count
            )

            return (job, message_id_str)

        except Exception as e:
            self.logger.error(
                "error_reading_job",
                worker_id=worker_id,
                error=str(e),
                exc_info=True
            )
            raise

    async def ack_job(self, message_id: str) -> None:
        """
        Confirms successful job processing.

        Args:
            message_id: Redis message ID
        """
        await self.redis.xack(
            self.stream_name,
            self.consumer_group,
            message_id
        )

        self.logger.info(
            "job_acknowledged",
            message_id=message_id
        )

    async def nack_job(
        self,
        message_id: str,
        job: ProcessingJob,
        error: str,
        requeue: bool = True
    ) -> None:
        """
        Rejects a job.

        Args:
            message_id: Redis message ID
            job: ProcessingJob that failed
            error: Error description
            requeue: If True, re-add to stream for retry
        """
        # ACK to remove from pending list
        await self.redis.xack(
            self.stream_name,
            self.consumer_group,
            message_id
        )

        if requeue:
            # Increment retry count and re-enqueue
            job.retry_count += 1
            await self.add_job(job)

            self.logger.warning(
                "job_requeued",
                job_id=job.job_id,
                message_id=message_id,
                retry_count=job.retry_count,
                error=error
            )
        else:
            # Send to Dead Letter Queue if configured
            if self.dead_letter_queue is not None:
                dlq_message_id = await self.dead_letter_queue.send_to_dlq(
                    job=job,
                    error=error,
                    original_message_id=message_id,
                    original_stream=self.stream_name
                )

                self.logger.error(
                    "job_rejected_sent_to_dlq",
                    job_id=job.job_id,
                    message_id=message_id,
                    retry_count=job.retry_count,
                    error=error,
                    dlq_message_id=dlq_message_id
                )
            else:
                # No DLQ configured, just log
                self.logger.error(
                    "job_rejected",
                    job_id=job.job_id,
                    message_id=message_id,
                    retry_count=job.retry_count,
                    error=error
                )

    async def reclaim_stalled_jobs(self, idle_time_ms: int = 300000) -> int:
        """
        Reclaims jobs that have been pending for more than idle_time_ms.
        Useful when a worker crashes and leaves jobs pending.

        Args:
            idle_time_ms: Time in milliseconds (default: 5 min)

        Returns:
            Number of jobs reclaimed
        """
        # Get pending entries
        pending = await self.redis.xpending_range(
            self.stream_name,
            self.consumer_group,
            min='-',
            max='+',
            count=100
        )

        reclaimed_count = 0

        for entry in pending:
            # entry: {'message_id', 'consumer', 'time_since_delivered', 'times_delivered'}
            if entry['time_since_delivered'] > idle_time_ms:
                # XCLAIM to reassign to another worker
                await self.redis.xclaim(
                    self.stream_name,
                    self.consumer_group,
                    'reclaim_service',  # temporary consumername
                    min_idle_time=idle_time_ms,
                    message_ids=[entry['message_id']]
                )

                # Immediately ACK so it becomes available again
                await self.redis.xack(
                    self.stream_name,
                    self.consumer_group,
                    entry['message_id']
                )

                reclaimed_count += 1

                self.logger.warning(
                    "stalled_job_reclaimed",
                    message_id=entry['message_id'],
                    original_consumer=entry['consumer'],
                    idle_time_ms=entry['time_since_delivered']
                )

        if reclaimed_count > 0:
            self.logger.info(
                "reclaim_operation_completed",
                reclaimed_count=reclaimed_count
            )

        return reclaimed_count

    async def create_consumer_group(self) -> None:
        """Creates the consumer group if it doesn't exist."""
        try:
            await self.redis.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id='0',
                mkstream=True
            )
            self.logger.info(
                "consumer_group_created",
                group=self.consumer_group
            )
        except Exception as e:
            # BUSYGROUP: Consumer group already exists
            if 'BUSYGROUP' in str(e):
                self.logger.debug(
                    "consumer_group_already_exists",
                    group=self.consumer_group
                )
            else:
                self.logger.error(
                    "error_creating_consumer_group",
                    group=self.consumer_group,
                    error=str(e)
                )
                raise

    async def get_pending_count(self, worker_id: Optional[str] = None) -> int:
        """
        Returns the number of pending jobs.

        Args:
            worker_id: If specified, count only for that worker

        Returns:
            Number of pending jobs
        """
        pending_info = await self.redis.xpending(
            self.stream_name,
            self.consumer_group
        )

        # pending_info: {'pending': count, 'min': id, 'max': id, 'consumers': [...]}
        return pending_info.get('pending', 0)

    async def get_stream_length(self) -> int:
        """Returns total number of messages in the stream."""
        length = await self.redis.xlen(self.stream_name)
        return length
