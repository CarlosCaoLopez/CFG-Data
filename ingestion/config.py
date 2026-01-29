# Ingestion configuration
# API keys, rate limits, and ingestion parameters

from pydantic_settings import BaseSettings
from pydantic import Field, computed_field
from functools import lru_cache


class IngestionSettings(BaseSettings):
    """Main ingestion settings - loads from .env file"""

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }

    # ===================
    # API Keys
    # ===================
    polygonscan_api_key: str = Field(
        default="",
        alias="POLYGONSCAN_API_KEY"
    )

    # ===================
    # Redis
    # ===================
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        alias="REDIS_URL"
    )

    # ===================
    # Rate Limits (from .env)
    # ===================
    polymarket_requests_per_minute: int = Field(
        default=100,
        alias="POLYMARKET_REQUESTS_PER_MINUTE"
    )
    polygonscan_requests_per_second: int = Field(
        default=5,
        alias="POLYGONSCAN_REQUESTS_PER_SECOND"
    )

    # ===================
    # API Base URLs (constants)
    # ===================
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    polygonscan_api_url: str = "https://api.polygonscan.com/api"

    # ===================
    # Retry Configuration
    # ===================
    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    timeout_seconds: float = 30.0
    backoff_multiplier: float = 2.0

    # ===================
    # Backfill Configuration
    # ===================
    backfill_default_days: int = 180
    backfill_min_volume_usdc: float = 50000.0
    backfill_batch_size: int = 50
    checkpoint_file: str = ".backfill_checkpoint.json"

    # ===================
    # Computed Properties
    # ===================
    @computed_field
    @property
    def polymarket_delay_seconds(self) -> float:
        """Minimum delay between Polymarket API requests"""
        return 60.0 / self.polymarket_requests_per_minute

    @computed_field
    @property
    def polygonscan_delay_seconds(self) -> float:
        """Minimum delay between Polygonscan API requests"""
        return 1.0 / self.polygonscan_requests_per_second


@lru_cache
def get_settings() -> IngestionSettings:
    """
    Cached settings loader.
    Call this function to get the singleton settings instance.

    Usage:
        from ingestion.config import get_settings
        settings = get_settings()
        print(settings.gamma_api_url)
    """
    return IngestionSettings()
