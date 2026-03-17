from fastapi import APIRouter
from dashboard import l1_stats_client

router = APIRouter(prefix="/api/config", tags=["config"])


@router.get("/categories")
async def get_categories():
    cats = await l1_stats_client.get_categories()
    if cats is None:
        return {"categories": [], "l1_available": False}
    return {"categories": cats, "l1_available": True}


@router.post("/categories")
async def set_categories(body: dict):
    cats = body.get("categories", [])
    return await l1_stats_client.post_categories(cats)
