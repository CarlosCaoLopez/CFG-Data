"""
Internal stats server for the ingestion container.

Runs on port 9001 (internal to proyect_net, not exposed to host).
Exposes L1 MarketCloseListener in-memory stats to the dashboard service.

Usage: started automatically by ingestion/main.py as a 4th asyncio task.
"""

from fastapi import FastAPI, HTTPException

stats_app = FastAPI(title="Ingestion Stats", docs_url=None, redoc_url=None)

_l1_ref = None
_l2_ref = None
_registry_ref = None


def set_listeners(l1, l2=None, registry=None) -> None:
    global _l1_ref, _l2_ref, _registry_ref
    _l1_ref = l1
    _l2_ref = l2
    _registry_ref = registry


@stats_app.get("/stats")
async def get_stats() -> dict:
    if _l1_ref is None:
        return {"error": "L1 not initialized yet"}

    tracked_list = [
        {
            "market_id": market_id,
            "question": meta.get("question", ""),
            "category": meta.get("category"),
            "tags": meta.get("tags", []),
            "volume_usdc": meta.get("volume_usdc", 0),
            "end_date": meta.get("end_date").isoformat() if meta.get("end_date") else None,
        }
        for market_id, meta in _l1_ref._tracked_markets.items()
    ]

    l2_stats = {}
    if _l2_ref is not None and hasattr(_l2_ref, "stats"):
        l2_stats = _l2_ref.stats

    return {
        "l1": {**_l1_ref.stats, "tracked_market_list": tracked_list},
        "l2": l2_stats,
    }


@stats_app.get("/config/categories")
async def get_categories() -> dict:
    if _registry_ref is None:
        return {"categories": []}
    return {"categories": sorted(_registry_ref.categories)}


@stats_app.post("/config/categories")
async def set_categories(body: dict) -> dict:
    if _registry_ref is None:
        raise HTTPException(status_code=503, detail="Registry not initialized")
    new_cats = body.get("categories", [])
    # Registry persists to Redis + notifies all subscribers (L1 + L2) live.
    await _registry_ref.set(new_cats)
    return {"ok": True, "categories": sorted(_registry_ref.categories)}


@stats_app.get("/health")
async def health() -> dict:
    return {"status": "ok", "l1_initialized": _l1_ref is not None}
