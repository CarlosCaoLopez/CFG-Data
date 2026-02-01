# Retry Handler - Exponential backoff retry logic
# Centralizes retry decision logic and backoff delay calculations

import random
import structlog
from datetime import datetime
from typing import Optional

from contracts.processing_job import ProcessingJob
from queue_system.config import QueueConfig

logger = structlog.get_logger(__name__)


class RetryHandler:
    """
    Handles retry logic with exponential backoff for failed jobs.

    Uses on-demand calculation (stateless) to determine if a job should be retried
    and calculates appropriate backoff delays with jitter to prevent thundering herd.
    """

    def __init__(self, config: QueueConfig):
        """
        Initialize retry handler with queue configuration.

        Args:
            config: Queue configuration containing retry parameters
        """
        self.config = config
        self.max_retries = config.max_retries
        self.backoff_base = config.retry_backoff_base
        self.max_delay = config.retry_max_delay

        logger.info(
            "retry_handler_initialized",
            max_retries=self.max_retries,
            backoff_base=self.backoff_base,
            max_delay=self.max_delay
        )

    def should_retry(self, job: ProcessingJob) -> bool:
        """
        Determine if a job should be retried based on retry count.

        Args:
            job: The processing job to check

        Returns:
            True if job should be retried, False if max retries exceeded
        """
        should_retry = job.retry_count < self.max_retries

        logger.info(
            "retry_decision",
            job_id=job.job_id,
            retry_count=job.retry_count,
            max_retries=self.max_retries,
            should_retry=should_retry
        )

        return should_retry

    def calculate_backoff_delay(self, retry_count: int) -> float:
        """
        Calculate exponential backoff delay with jitter.

        Formula: delay = min(base^retry_count, max_delay) + jitter
        Jitter: random value between 0 and 1 second to prevent thundering herd

        Args:
            retry_count: Number of times the job has been retried

        Returns:
            Delay in seconds (float)
        """
        # Calculate base exponential delay
        base_delay = self.backoff_base ** retry_count

        # Cap at max_delay
        capped_delay = min(base_delay, self.max_delay)

        # Add jitter (0-1 seconds) to prevent thundering herd
        jitter = random.uniform(0, 1)
        final_delay = capped_delay + jitter

        logger.debug(
            "backoff_calculated",
            retry_count=retry_count,
            base_delay=base_delay,
            capped_delay=capped_delay,
            jitter=jitter,
            final_delay=final_delay
        )

        return final_delay

    def get_retry_metadata(
        self,
        job: ProcessingJob,
        error: str,
        next_retry_delay: Optional[float] = None
    ) -> dict:
        """
        Create structured metadata for retry logging and observability.

        Args:
            job: The processing job being retried
            error: Error message from the failed attempt
            next_retry_delay: Calculated delay before next retry (if applicable)

        Returns:
            Dictionary with retry metadata for logging
        """
        metadata = {
            "job_id": job.job_id,
            "job_type": job.job_type.value,
            "retry_count": job.retry_count,
            "max_retries": self.max_retries,
            "error": error,
            "created_at": job.created_at.isoformat(),
            "will_retry": self.should_retry(job),
        }

        if next_retry_delay is not None:
            metadata["next_retry_delay_seconds"] = round(next_retry_delay, 2)
            metadata["estimated_retry_at"] = datetime.utcnow().isoformat()

        return metadata
