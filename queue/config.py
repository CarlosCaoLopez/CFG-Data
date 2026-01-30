from pydantic_settings import BaseSettings

# Queue configuration
# Redis connection and queue parameters
class QueueConfig(BaseSettings):
    redis_url: str = "redis://localhost:6379/0"

    # Streams
    raw_data_stream_name: str = "raw_data"
    jobs_stream_name: str = "jobs_stream"

    # Consumer group
    consumer_group_name: str = "workers"
    
    # Limits
    max_pending_jobs: int = 1000
    job_timeout_seconds: int = 300  # 5 min
    
    # Retry
    max_retries: int = 3
    retry_backoff_base: float = 2.0
    retry_max_delay: float = 300.0  # 5 min

    # Rate Limiting
    max_jobs_per_second: float = 10.0
    rate_limiter_burst_size: int = 20

    # Dead Letter Queue
    dlq_retention_days: int = 30

