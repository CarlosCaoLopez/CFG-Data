from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from dashboard.dependencies import get_session
from dashboard.queries import variable_queries as q

router = APIRouter()


@router.get("/api/variables")
async def list_variables(
    market_id: str | None = None,
    page: int = 1,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
):
    if market_id:
        return await q.get_variables_detail(session, market_id, page=page, limit=limit)
    summary = await q.get_variables_summary(session)
    return {"summary": summary, "total": len(summary)}
