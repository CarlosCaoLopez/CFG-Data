# Database connection pool management
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from database.base import Base

# Async mode
DATABASE_URL = "postgresql+asyncpg://insider:insider_pass@localhost:5433/insider_detection"

engine = create_async_engine(
    DATABASE_URL,
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