# Dead Letter Queue - Failed job handling
# Stores permanently failed jobs in a separate Redis Stream with 30-day retention

import json
import structlog
from datetime import datetime, timedelta
from typing import Optional
from redis.asyncio import Redis

from contracts.processing_job import ProcessingJob

logger = structlog.get_logger(__name__)


class DeadLetterQueue:
    """
    Dead Letter Queue for storing permanently failed jobs.

    Uses Redis Streams to persist jobs that exceeded max retry attempts,
    enabling manual review, reprocessing, and debugging.
    """

    def __init__(
        self,
        redis_client: Redis,
        stream_name: str = "dead_letter_queue",
        retention_days: int = 30
    ):
        """
        Initialize Dead Letter Queue.

        Args:
            redis_client: Async Redis client
            stream_name: Name of the Redis Stream for DLQ
            retention_days: Days to retain failed jobs before auto-cleanup
        """
        self.redis = redis_client
        self.stream_name = stream_name
        self.retention_days = retention_days

        logger.info(
            "dead_letter_queue_initialized",
            stream_name=stream_name,
            retention_days=retention_days
        )

    async def send_to_dlq(
        self,
        job: ProcessingJob,
        error: str,
        original_message_id: str,
        original_stream: str = "jobs_stream"
    ) -> str:
        """
        Send a permanently failed job to the Dead Letter Queue.

        Args:
            job: The failed processing job
            error: Error message from the final failed attempt
            original_message_id: Message ID from the original stream
            original_stream: Name of the stream where the job originated

        Returns:
            Message ID of the DLQ entry
        """
        # Prepare DLQ entry with full context
        dlq_entry = {
            "job_id": job.job_id,
            "job_type": job.job_type.value,
            "job_data": job.model_dump_json(),  # Full job serialization
            "error": error,
            "retry_count": job.retry_count,
            "original_stream": original_stream,
            "original_message_id": original_message_id,
            "failed_at": datetime.utcnow().isoformat(),
            "created_at": job.created_at.isoformat(),
        }

        # Add to DLQ stream
        message_id = await self.redis.xadd(
            self.stream_name,
            dlq_entry
        )

        # Auto-trim stream to maintain retention policy
        await self._trim_old_entries()

        logger.warning(
            "job_sent_to_dlq",
            job_id=job.job_id,
            job_type=job.job_type.value,
            retry_count=job.retry_count,
            error=error[:100],  # Truncate long errors
            dlq_message_id=message_id.decode() if isinstance(message_id, bytes) else message_id
        )

        return message_id.decode() if isinstance(message_id, bytes) else message_id

    async def get_failed_jobs(
        self,
        count: int = 10,
        start_id: str = "-"
    ) -> list[tuple[str, dict]]:
        """
        Retrieve failed jobs from the DLQ for review.

        Args:
            count: Maximum number of jobs to retrieve
            start_id: Starting message ID (use "-" for beginning, "+" for end)

        Returns:
            List of tuples: [(message_id, job_data), ...]
        """
        try:
            # Read from DLQ stream
            messages = await self.redis.xread(
                {self.stream_name: start_id},
                count=count
            )

            if not messages:
                return []

            # Extract and decode messages
            stream_name, entries = messages[0]
            results = []

            for message_id, fields in entries:
                # Decode Redis bytes to strings
                decoded_fields = {
                    k.decode() if isinstance(k, bytes) else k:
                    v.decode() if isinstance(v, bytes) else v
                    for k, v in fields.items()
                }

                message_id_str = message_id.decode() if isinstance(message_id, bytes) else message_id
                results.append((message_id_str, decoded_fields))

            logger.info(
                "dlq_jobs_retrieved",
                count=len(results),
                start_id=start_id
            )

            return results

        except Exception as e:
            logger.error(
                "dlq_retrieval_failed",
                error=str(e),
                exc_info=True
            )
            return []

    async def reprocess_job(self, dlq_message_id: str) -> Optional[ProcessingJob]:
        """
        Extract a job from DLQ for manual reprocessing.

        Args:
            dlq_message_id: Message ID in the DLQ stream

        Returns:
            Reconstructed ProcessingJob, or None if not found
        """
        try:
            # Read specific message from DLQ
            messages = await self.redis.xrange(
                self.stream_name,
                min=dlq_message_id,
                max=dlq_message_id,
                count=1
            )

            if not messages:
                logger.warning(
                    "dlq_job_not_found",
                    message_id=dlq_message_id
                )
                return None

            message_id, fields = messages[0]

            # Decode and deserialize job
            decoded_fields = {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in fields.items()
            }

            job_data_json = decoded_fields.get("job_data")
            if not job_data_json:
                logger.error(
                    "dlq_job_missing_data",
                    message_id=dlq_message_id
                )
                return None

            # Reconstruct ProcessingJob
            job = ProcessingJob.model_validate_json(job_data_json)

            # Reset retry count for reprocessing
            job.retry_count = 0

            logger.info(
                "dlq_job_reprocessed",
                job_id=job.job_id,
                original_retry_count=decoded_fields.get("retry_count")
            )

            return job

        except Exception as e:
            logger.error(
                "dlq_reprocess_failed",
                message_id=dlq_message_id,
                error=str(e),
                exc_info=True
            )
            return None

    async def get_dlq_count(self) -> int:
        """
        Get the number of jobs currently in the DLQ.

        Returns:
            Count of DLQ entries
        """
        try:
            length = await self.redis.xlen(self.stream_name)
            return length
        except Exception as e:
            logger.error(
                "dlq_count_failed",
                error=str(e),
                exc_info=True
            )
            return 0

    async def _trim_old_entries(self) -> int:
        """
        Remove DLQ entries older than retention_days.

        Uses Redis XTRIM with MINID to remove old messages efficiently.

        Returns:
            Number of entries trimmed
        """
        try:
            # Calculate cutoff timestamp (retention_days ago)
            cutoff_time = datetime.utcnow() - timedelta(days=self.retention_days)

            # Redis Stream IDs are in format: timestamp-sequence
            # Convert cutoff time to milliseconds since epoch
            cutoff_ms = int(cutoff_time.timestamp() * 1000)
            min_id = f"{cutoff_ms}-0"

            # Trim entries older than min_id
            trimmed = await self.redis.xtrim(
                self.stream_name,
                minid=min_id,
                approximate=True  # Use approximate for better performance
            )

            if trimmed > 0:
                logger.info(
                    "dlq_trimmed",
                    entries_removed=trimmed,
                    retention_days=self.retention_days,
                    cutoff_time=cutoff_time.isoformat()
                )

            return trimmed

        except Exception as e:
            logger.error(
                "dlq_trim_failed",
                error=str(e),
                exc_info=True
            )
            return 0

    async def cleanup_old_entries(self, days: Optional[int] = None) -> int:
        """
        Manually trigger cleanup of old DLQ entries.

        Args:
            days: Override retention period (uses default if not provided)

        Returns:
            Number of entries removed
        """
        if days is not None:
            original_retention = self.retention_days
            self.retention_days = days

        trimmed = await self._trim_old_entries()

        if days is not None:
            self.retention_days = original_retention

        return trimmed
