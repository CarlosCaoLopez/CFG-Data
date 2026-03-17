from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def get_markets(
    session: AsyncSession,
    page: int = 1,
    limit: int = 50,
    closed: bool | None = None,
    category: str | None = None,
) -> dict:
    offset = (page - 1) * limit
    filters = []
    params: dict = {"limit": limit, "offset": offset}

    if closed is not None:
        filters.append("m.closed = :closed")
        params["closed"] = closed
    if category:
        filters.append(":category = ANY(m.categories)")
        params["category"] = category

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    rows = (await session.execute(text(f"""
        SELECT
          m.market_id, m.question, m.end_date, m.closed,
          m.volume_usdc, m.categories, m.tags, m.winning_outcome, m.updated_at,
          COUNT(DISTINCT t.id)  AS trades_count,
          COUNT(DISTINCT it.id) AS insiders_count
        FROM markets m
        LEFT JOIN trades t         ON t.market_id = m.market_id
        LEFT JOIN insiders_trades it ON it.market_id = m.market_id
        {where}
        GROUP BY m.market_id
        ORDER BY m.updated_at DESC
        LIMIT :limit OFFSET :offset
    """), params)).fetchall()

    total_row = (await session.execute(text(f"""
        SELECT COUNT(*) FROM markets m {where}
    """), {k: v for k, v in params.items() if k not in ("limit", "offset")})).scalar()

    return {
        "items": [_market_row(r) for r in rows],
        "total": total_row or 0,
        "page": page,
    }


async def get_market_detail(session: AsyncSession, market_id: str) -> dict | None:
    row = (await session.execute(text("""
        SELECT m.*, COUNT(DISTINCT t.id) AS trades_count, COUNT(DISTINCT it.id) AS insiders_count
        FROM markets m
        LEFT JOIN trades t ON t.market_id = m.market_id
        LEFT JOIN insiders_trades it ON it.market_id = m.market_id
        WHERE m.market_id = :mid
        GROUP BY m.market_id
    """), {"mid": market_id})).fetchone()
    if not row:
        return None

    cmv_rows = (await session.execute(text("""
        SELECT wallet_id, delta_t, age, winning_volume, alpha_slippage, entropy
        FROM closed_market_variables WHERE market_id = :mid
        ORDER BY delta_t ASC LIMIT 100
    """), {"mid": market_id})).fetchall()

    result = _market_row(row)
    result["cmv"] = [dict(r._mapping) for r in cmv_rows]
    return result


def _market_row(r) -> dict:
    m = dict(r._mapping)
    if m.get("end_date"):
        m["end_date"] = m["end_date"].isoformat()
    if m.get("updated_at"):
        m["updated_at"] = m["updated_at"].isoformat()
    m["volume_usdc"] = float(m.get("volume_usdc") or 0)
    m["trades_count"] = int(m.get("trades_count") or 0)
    m["insiders_count"] = int(m.get("insiders_count") or 0)
    return m
