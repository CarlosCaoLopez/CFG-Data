from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from dashboard.dependencies import get_session
from dashboard.queries import overview_queries as q

router = APIRouter()


@router.get("/api/overview")
async def overview(session: AsyncSession = Depends(get_session)):
    kpis = await q.get_kpis(session)
    categories = await q.get_top_categories(session)
    detections = await q.get_recent_detections(session)
    return {**kpis, "top_categories": categories, "recent_detections": detections}
