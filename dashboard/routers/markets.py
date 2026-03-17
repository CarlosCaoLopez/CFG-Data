from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from dashboard.dependencies import get_session
from dashboard.queries import market_queries as q

router = APIRouter()


@router.get("/api/markets")
async def list_markets(
    page: int = 1,
    limit: int = 50,
    closed: str | None = Query(None, description="true|false|all"),
    category: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    closed_bool = None
    if closed == "true":
        closed_bool = True
    elif closed == "false":
        closed_bool = False
    return await q.get_markets(session, page=page, limit=limit, closed=closed_bool, category=category)


@router.get("/api/markets/{market_id}")
async def market_detail(market_id: str, session: AsyncSession = Depends(get_session)):
    data = await q.get_market_detail(session, market_id)
    if not data:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Market not found")
    return data
