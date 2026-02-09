# Database connection pool management
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from database.base import Base

# Async mode (para FastAPI, scripts async, etc.)
DATABASE_URL_ASYNC = "postgresql+asyncpg://insider:insider_pass@localhost:5433/insider_detection"

engine = create_async_engine(
    DATABASE_URL_ASYNC,
    pool_size=2,        # Only two permanent connections per worker. Be careful with scalability
    max_overflow=4,
    pool_timeout=30,
    pool_recycle=1800,
    echo=False
)
async_session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

async def get_session():
    async with async_session() as session:
        yield session

# Sync mode (para Celery workers - evita problemas con event loops)
DATABASE_URL_SYNC = "postgresql+psycopg2://insider:insider_pass@localhost:5433/insider_detection"

sync_engine = create_engine(
    DATABASE_URL_SYNC,
    pool_size=2,
    max_overflow=4,
    pool_timeout=30,
    pool_recycle=1800,
    echo=False
)
sync_session = sessionmaker(sync_engine, expire_on_commit=False, class_=Session)

@contextmanager
def get_sync_session():
    """Context manager para sesiones síncronas (Celery tasks)"""
    session = sync_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()