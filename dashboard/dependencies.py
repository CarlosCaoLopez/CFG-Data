from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from redis.asyncio import Redis
from dashboard.config import DATABASE_URL_ASYNC, REDIS_URL

_engine = create_async_engine(
    DATABASE_URL_ASYNC,
    pool_size=10,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False,
)
_session_factory = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

_redis: Redis | None = None


async def get_session() -> AsyncSession:
    async with _session_factory() as session:
        yield session


async def get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis
