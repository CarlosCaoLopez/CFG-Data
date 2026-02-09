from pydantic_settings import BaseSettings
from pydantic import ConfigDict

# Queue configuration
# Redis connection and queue parameters
class QueueConfig(BaseSettings):
    model_config = ConfigDict(extra='allow')

    redis_url: str = "redis://localhost:6379/0"

    # Streams (A → B)
    raw_data_stream_name: str = "raw_data"
    closed_markets_stream_name: str = "closed_markets"
    insider_trades_stream_name: str = "insider_trades"
    insider_commands_stream_name: str = "insider_commands"

    # Streams (B → C)
    jobs_stream_name: str = "jobs_stream"
    results_stream_name: str = "results_stream"

    # Consumer groups
    consumer_group_name: str = "workers"
    consumer_name: str = "orchestrator"
    orchestrator_consumer_group: str = "scheduler"
    insider_trades_consumer_group: str = "tasks"

    # Limits
    max_pending_jobs: int = 1000
    job_timeout_seconds: int = 300  # 5 min
    job_claim_timeout_ms: int = 5000
    job_processing_timeout_ms: int = 30000

    # Retry
    max_retries: int = 3
    retry_backoff_base: float = 2.0
    retry_max_delay: float = 300.0  # 5 min
    initial_retry_delay_seconds: float = 1.0
    max_retry_delay_seconds: float = 300.0

    # Rate Limiting
    max_jobs_per_second: float = 10.0
    rate_limiter_burst_size: int = 20
    rate_limit_requests_per_second: int = 100
    rate_limit_burst_size: int = 10

    # Orchestrator
    orchestrator_batch_size: int = 10
    orchestrator_poll_interval_seconds: float = 1.0

    # Dead Letter Queue
    dlq_retention_days: int = 30