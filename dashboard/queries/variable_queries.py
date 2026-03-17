from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def get_variables_summary(session: AsyncSession) -> list[dict]:
    rows = (await session.execute(text("""
        SELECT cmv.market_id, m.question,
          COUNT(DISTINCT cmv.wallet_id) AS wallet_count,
          COUNT(*) AS row_count,
          ROUND(AVG(cmv.delta_t)::numeric, 2) AS avg_delta_t,
          ROUND(AVG(cmv.entropy)::numeric, 4) AS avg_entropy,
          ROUND(AVG(cmv.alpha_slippage)::numeric, 4) AS avg_slippage
        FROM closed_market_variables cmv
        JOIN markets m ON m.market_id = cmv.market_id
        GROUP BY cmv.market_id, m.question
        ORDER BY row_count DESC
    """))).fetchall()
    return [dict(r._mapping) for r in rows]


async def get_variables_detail(
    session: AsyncSession, market_id: str, page: int = 1, limit: int = 100
) -> dict:
    offset = (page - 1) * limit
    rows = (await session.execute(text("""
        SELECT wallet_id, trade_id, delta_t, age, winning_volume, alpha_slippage, entropy
        FROM closed_market_variables
        WHERE market_id = :mid
        ORDER BY delta_t ASC
        LIMIT :limit OFFSET :offset
    """), {"mid": market_id, "limit": limit, "offset": offset})).fetchall()
    total = (await session.execute(text(
        "SELECT COUNT(*) FROM closed_market_variables WHERE market_id = :mid"
    ), {"mid": market_id})).scalar()
    return {
        "items": [_cmv_row(r) for r in rows],
        "total": total or 0,
        "page": page,
    }


def _cmv_row(r) -> dict:
    m = dict(r._mapping)
    for f in ("delta_t", "age", "winning_volume", "alpha_slippage", "entropy"):
        if m.get(f) is not None:
            m[f] = float(m[f])
    return m
