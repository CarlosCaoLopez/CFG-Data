"""HTTP client to fetch L1 MarketCloseListener stats from the ingestion container."""

import httpx
from dashboard.config import INGESTION_STATS_URL


async def get_l1_stats() -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{INGESTION_STATS_URL}/stats")
            r.raise_for_status()
            return r.json()
    except Exception:
        return None


async def get_categories() -> list[str] | None:
    """Returns list of categories on success, None if L1 is unreachable."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{INGESTION_STATS_URL}/config/categories")
            r.raise_for_status()
            return r.json().get("categories", [])
    except Exception:
        return None


async def post_categories(categories: list[str]) -> dict:
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.post(
                f"{INGESTION_STATS_URL}/config/categories",
                json={"categories": categories},
            )
            r.raise_for_status()
            return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}
