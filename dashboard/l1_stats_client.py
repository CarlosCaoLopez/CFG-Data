"""HTTP client to fetch L1 MarketCloseListener stats from the ingestion container."""

import asyncio
import time

import httpx
from dashboard.config import INGESTION_STATS_URL

# Cliente reutilizado (con keep-alive) en vez de abrir uno nuevo por request,
# y caché TTL: el pipeline page hace polling cada 8s y las stats L1 no cambian
# tan rápido. Si ingestion no responde, el timeout de 2s solo se paga 1 vez
# cada _L1_TTL segundos, no en cada refresco de cada cliente.
_client: httpx.AsyncClient | None = None
_L1_TTL = 8.0
_l1_cache: dict | None = None
_l1_cache_at: float = 0.0
_l1_lock = asyncio.Lock()


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=2.0)
    return _client


async def get_l1_stats() -> dict | None:
    global _l1_cache, _l1_cache_at

    if _l1_cache is not None and (time.monotonic() - _l1_cache_at) < _L1_TTL:
        return _l1_cache

    async with _l1_lock:
        if _l1_cache is not None and (time.monotonic() - _l1_cache_at) < _L1_TTL:
            return _l1_cache
        try:
            r = await _get_client().get(f"{INGESTION_STATS_URL}/stats")
            r.raise_for_status()
            _l1_cache = r.json()
        except Exception:
            _l1_cache = None
        _l1_cache_at = time.monotonic()
        return _l1_cache


async def get_categories() -> list[str] | None:
    """Returns list of categories on success, None if L1 is unreachable."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{INGESTION_STATS_URL}/config/categories")
            r.raise_for_status()
            return r.json().get("categories", [])
    except Exception:
        return None


async def get_monitored_wallet(wallet: str) -> dict | None:
    """L2 live monitoring state for a wallet (see ingestion/stats_server.py). None if unreachable."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{INGESTION_STATS_URL}/monitored/{wallet}")
            r.raise_for_status()
            return r.json()
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
