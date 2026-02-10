"""
Shared fixtures for integration tests.
Requires running PostgreSQL (localhost:5433) and Redis (localhost:6379).
"""
from datetime import datetime, timezone

import pytest
import pytest_asyncio
import redis
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from database.base import Base
from database.connection import DATABASE_URL_ASYNC, get_session
from api.main import app


# ── Database ────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine(DATABASE_URL_ASYNC, echo=False)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as session:
        yield session
        await session.rollback()
    await engine.dispose()


TEST_WALLETS = [
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "0xcccccccccccccccccccccccccccccccccccccccc",
]
TEST_MARKET_ID = "0xTEST_MARKET_000000000000000000000000000000000000000000000000000"


@pytest_asyncio.fixture
async def clean_wallets(db_session):
    """Delete test wallets before and after each test."""
    for addr in TEST_WALLETS:
        await db_session.execute(text("DELETE FROM wallets WHERE address = :addr"), {"addr": addr})
    await db_session.commit()
    yield TEST_WALLETS
    for addr in TEST_WALLETS:
        await db_session.execute(text("DELETE FROM wallets WHERE address = :addr"), {"addr": addr})
    await db_session.commit()


@pytest_asyncio.fixture
async def seed_market(db_session):
    """Insert a test market and clean it up after."""
    await db_session.execute(
        text(
            "INSERT INTO markets (market_id, question, end_date, closed, volume_usdc, categories, updated_at) "
            "VALUES (:mid, :q, :ed, true, 100000, :cats, NOW()) "
            "ON CONFLICT (market_id) DO NOTHING"
        ),
        {
            "mid": TEST_MARKET_ID,
            "q": "Test market for insider detection",
            "ed": datetime(2027, 1, 1, tzinfo=timezone.utc),
            "cats": ["TestCat"],
        },
    )
    await db_session.commit()
    yield TEST_MARKET_ID
    await db_session.execute(text("DELETE FROM markets WHERE market_id = :mid"), {"mid": TEST_MARKET_ID})
    await db_session.commit()


# ── Redis ───────────────────────────────────────────────────────────

@pytest.fixture
def redis_client():
    client = redis.from_url("redis://localhost:6379/0")
    yield client
    client.close()


@pytest.fixture
def clean_stream(redis_client):
    """Delete the insider_commands stream before and after each test."""
    redis_client.delete("insider_commands")
    yield
    redis_client.delete("insider_commands")


# ── FastAPI test client (overrides get_session to use test engine) ──

@pytest_asyncio.fixture
async def api_client():
    """
    Creates a fresh engine per test to avoid event loop conflicts.
    Overrides FastAPI's get_session dependency so the API handler
    uses a session bound to this test's event loop.
    """
    engine = create_async_engine(DATABASE_URL_ASYNC, echo=False)
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def _override_get_session():
        async with factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_get_session
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()
    await engine.dispose()


# ── Sample data builders ────────────────────────────────────────────

def make_batch(market_id: str, wallets: list[dict]) -> dict:
    return {"market_id": market_id, "wallets": wallets}


def make_wallet(address: str, categories: list[str], command: str = "ADD_WALLET") -> dict:
    return {"wallet": address, "categories_insider": categories, "command_type": command}
