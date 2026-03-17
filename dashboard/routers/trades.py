from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from dashboard.dependencies import get_session
from dashboard.queries import trade_queries as q

router = APIRouter()


@router.get("/api/trades")
async def list_trades(
    page: int = 1,
    limit: int = 100,
    wallet: str | None = None,
    market_id: str | None = None,
    side: str | None = None,
    table: str = Query("insider", description="insider | normal | all"),
    session: AsyncSession = Depends(get_session),
):
    return await q.get_trades(session, page=page, limit=limit, wallet=wallet, market_id=market_id, side=side, table=table)
