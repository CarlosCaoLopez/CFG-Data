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
    # Etherscan API V2 key (unified for all chains including Polygon)
    etherscan_api_key: str = Field(
        default="",
        validation_alias="ETHERSCAN_API_KEY"
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
    # Etherscan API V2 free tier limit is 3 calls/sec (not 5)
    polygonscan_requests_per_second: int = Field(
        default=3,
        alias="POLYGONSCAN_REQUESTS_PER_SECOND"
    )

    # ===================
    # API Base URLs (constants)
    # ===================
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    clob_api_url: str = "https://clob.polymarket.com"
    data_api_url: str = "https://data-api.polymarket.com"
    websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Etherscan API V2 - unified endpoint for all chains
    # V1 (deprecated): https://api.polygonscan.com/api
    # V2 (current): https://api.etherscan.io/v2/api with chainid parameter
    etherscan_api_url: str = "https://api.etherscan.io/v2/api"
    polygon_chain_id: int = 137  # Polygon PoS mainnet

    # ===================
    # CATEGORIAS_DE_INTERES (global filter)
    # Set via .env: CATEGORIAS_DE_INTERES="Sports,Crypto,US-current-affairs,Pop Culture"
    # ===================
    categorias_de_interes_str: str = Field(
        default="Sports,Crypto,US-current-affairs,Pop Culture",
        validation_alias="CATEGORIAS_DE_INTERES",
        description="Comma-separated tags/categories to monitor.",
    )

    @computed_field
    @property
    def categorias_de_interes(self) -> list[str]:
        """Parse comma-separated string into list"""
        return [s.strip() for s in self.categorias_de_interes_str.split(",") if s.strip()]

    # ===================
    # L1: Market Close Listener
    # ===================
    l1_market_poll_interval_seconds: int = Field(
        default=60,
        description="How often L1 checks tracked markets for closure (seconds)",
    )
    l1_gamma_discovery_interval_seconds: int = Field(
        default=900,
        description="How often L1 discovers markets via Gamma API (seconds, 15 min)",
    )
    l1_min_volume_usdc: float = Field(
        default=10000.0,
        description="Minimum volume for L1 to track a market",
    )

    # ===================
    # L2: Insider Trades Listener
    # ===================
    l2_poll_interval_seconds: int = Field(
        default=30,
        description="How often L2 polls for new insider wallet trades (seconds)",
    )
    l2_commands_stream: str = Field(
        default="insider_commands",
        description="Redis stream for T3 → L2 commands",
    )
    l2_commands_consumer_group: str = Field(
        default="ingestion_l2",
        description="Consumer group for insider_commands stream",
    )

    # ===================
    # Redis Streams (A → B)
    # ===================
    closed_markets_stream: str = Field(
        default="closed_markets",
        description="Redis stream for L1 closed market batches",
    )
    insider_trades_stream: str = Field(
        default="insider_trades",
        description="Redis stream for L2 insider trade data",
    )

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
    # Realtime Listener Configuration
    # ===================
    # WebSocket RPC endpoints for Polygon (fallback order)
    polygon_wss_endpoints: list[str] = Field(
        default=[
            "wss://polygon-bor-rpc.publicnode.com",
            "wss://rpc.ankr.com/polygon/ws",
            "wss://polygon.drpc.org",
        ],
        description="WebSocket RPC endpoints for Polygon (fallback order)"
    )

    # WebSocket reconnection settings
    ws_reconnect_delay_base: float = Field(
        default=1.0,
        description="Base delay for WebSocket reconnection (seconds)"
    )
    ws_reconnect_delay_max: float = Field(
        default=60.0,
        description="Maximum delay for WebSocket reconnection (seconds)"
    )
    ws_reconnect_max_attempts: int = Field(
        default=10,
        description="Max reconnection attempts before switching to polling"
    )

    # Polling settings (fallback mode)
    polling_interval_seconds: int = Field(
        default=15,
        description="Interval between polling requests when in fallback mode"
    )
    polling_blocks_per_request: int = Field(
        default=100,
        description="Number of blocks to query per polling request"
    )

    # Deduplication cache
    dedup_cache_size: int = Field(
        default=10000,
        description="Max number of tx_hashes to keep in dedup cache"
    )
    dedup_cache_ttl_seconds: int = Field(
        default=3600,
        description="TTL for dedup cache entries (1 hour)"
    )

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
