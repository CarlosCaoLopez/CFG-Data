# QueueConsumer - Abstract interface for Persona C to consume jobs

from abc import ABC, abstractmethod
from contracts.processing_job import ProcessingJob, JobResult


class QueueConsumer(ABC):
    """
    Abstract interface for consuming processing jobs from the queue.
    Used by Persona C (Workers) to receive jobs from Persona B (Queue/Orchestration).
    """

    @abstractmethod
    async def get_next_job(self, worker_id: str, timeout_ms: int = 5000) -> ProcessingJob | None:
        """
        Retrieves the next job from the queue, respecting priority order.
        Blocks until a job is available or timeout is reached.

        Args:
            worker_id: Unique identifier for this worker
            timeout_ms: Maximum time to wait for a job in milliseconds

        Returns:
            ProcessingJob if available, None if timeout reached

        Raises:
            QueueConnectionError: If Redis connection fails
        """
        pass

    @abstractmethod
    async def ack_job(self, job_id: str, result: JobResult) -> None:
        """
        Acknowledges successful job completion.
        Removes job from pending list and stores result.

        Args:
            job_id: ID of the completed job
            result: Processing result with metrics

        Raises:
            QueueConnectionError: If Redis connection fails
            JobNotFoundError: If job_id doesn't exist
        """
        pass

    @abstractmethod
    async def nack_job(self, job_id: str, error: str, retry: bool = True) -> None:
        """
        Rejects a job due to processing failure.

        Args:
            job_id: ID of the failed job
            error: Error message describing the failure
            retry: If True, re-queue the job for retry. If False, send to dead letter queue

        Raises:
            QueueConnectionError: If Redis connection fails
            JobNotFoundError: If job_id doesn't exist
        """
        pass

    @abstractmethod
    async def get_pending_count(self) -> int:
        """
        Returns the number of pending jobs in the queue.
        Useful for monitoring and load balancing.

        Returns:
            Number of jobs waiting to be processed

        Raises:
            QueueConnectionError: If Redis connection fails
        """
        pass
