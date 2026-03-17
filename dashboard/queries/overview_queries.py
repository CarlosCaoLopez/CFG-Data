from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def get_kpis(session: AsyncSession) -> dict:
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
