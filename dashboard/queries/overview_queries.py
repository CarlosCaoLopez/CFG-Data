import asyncio
import time

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

# Caché compartida de KPIs. Los 8 COUNT(*) son full scans caros y el SSE los
# pedía cada 5s por cada cliente conectado. Con esto se calculan como mucho una
# vez cada _KPIS_TTL segundos, y todos los clientes (SSE + /api/overview)
# comparten el mismo resultado. El lock evita el "thundering herd" cuando varias
# requests llegan a la vez con la caché expirada.
_KPIS_TTL = 20.0
_kpis_cache: dict | None = None
_kpis_cache_at: float = 0.0
_kpis_lock = asyncio.Lock()


async def _query_kpis(session: AsyncSession) -> dict:
    row = (await session.execute(text("""
        SELECT
          (SELECT COUNT(*) FROM wallets)                            AS total_wallets,
          (SELECT COUNT(*) FROM wallets WHERE insider = TRUE)       AS total_insiders,
          (SELECT COUNT(*) FROM markets)                            AS total_markets,
          (SELECT COUNT(*) FROM markets WHERE closed = TRUE)        AS closed_markets,
          (SELECT COUNT(*) FROM trades)                             AS total_trades,
          (SELECT COUNT(*) FROM insiders_trades)                    AS insider_trades,
          (SELECT COUNT(*) FROM closed_market_variables)            AS cmv_in_flight,
          (SELECT COUNT(*) FROM wallets WHERE insider = TRUE
           AND updated_at >= NOW() - INTERVAL '24 hours')          AS detections_last_24h
    """))).fetchone()
    return dict(row._mapping) if row else {}


async def get_kpis(session: AsyncSession) -> dict:
    global _kpis_cache, _kpis_cache_at

    if _kpis_cache is not None and (time.monotonic() - _kpis_cache_at) < _KPIS_TTL:
        return _kpis_cache

    async with _kpis_lock:
        # Otra corutina pudo refrescar la caché mientras esperábamos el lock.
        if _kpis_cache is not None and (time.monotonic() - _kpis_cache_at) < _KPIS_TTL:
            return _kpis_cache
        _kpis_cache = await _query_kpis(session)
        _kpis_cache_at = time.monotonic()
        return _kpis_cache


async def get_top_categories(session: AsyncSession, limit: int = 8) -> list[dict]:
    rows = (await session.execute(text("""
        SELECT unnest(categories_insider) AS category, COUNT(*) AS cnt
        FROM wallets WHERE insider = TRUE
        GROUP BY category ORDER BY cnt DESC LIMIT :limit
    """), {"limit": limit})).fetchall()
    return [{"category": r.category, "count": r.cnt} for r in rows]


async def get_recent_detections(session: AsyncSession, limit: int = 10) -> list[dict]:
    rows = (await session.execute(text("""
        SELECT address, categories_insider, marketids_insider, updated_at
        FROM wallets WHERE insider = TRUE
        ORDER BY updated_at DESC LIMIT :limit
    """), {"limit": limit})).fetchall()
    return [
        {
            "wallet": r.address,
            "categories": r.categories_insider or [],
            "markets_count": len(r.marketids_insider or []),
            "detected_at": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rows
    ]
