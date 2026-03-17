import os
import secrets

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://insider:insider_pass@localhost:5433/insider_detection",
)
DATABASE_URL_ASYNC = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
INGESTION_STATS_URL = os.environ.get("INGESTION_STATS_URL", "http://localhost:9001")

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
# Server secret for signing auth tokens — randomised per restart if not set
AUTH_SECRET = os.environ.get("AUTH_SECRET", secrets.token_hex(32))
AUTH_TOKEN_TTL = 3600  # seconds (1 hour)
