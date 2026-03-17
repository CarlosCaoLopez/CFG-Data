import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://insider:insider_pass@localhost:5433/insider_detection",
)
DATABASE_URL_ASYNC = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
INGESTION_STATS_URL = os.environ.get("INGESTION_STATS_URL", "http://localhost:9001")
