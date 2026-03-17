from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from dashboard.dependencies import get_session
from dashboard.queries import insider_queries as q

router = APIRouter()


@router.get("/api/insiders")
async def list_insiders(
    page: int = 1,
    limit: int = 50,
    category: str | None = None,
    session: AsyncSession = Depends(get_session),
):
    return await q.get_insiders(session, page=page, limit=limit, category=category)


@router.get("/api/insiders/{wallet}")
async def insider_detail(wallet: str, session: AsyncSession = Depends(get_session)):
    data = await q.get_insider_detail(session, wallet)
    if not data:
        raise HTTPException(status_code=404, detail="Wallet not found")
    return data
