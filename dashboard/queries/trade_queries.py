from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def get_trades(
    session: AsyncSession,
    page: int = 1,
    limit: int = 100,
    wallet: str | None = None,
    market_id: str | None = None,
    side: str | None = None,
    table: str = "insider",
) -> dict:
    offset = (page - 1) * limit
    params: dict = {"limit": limit, "offset": offset}

    def build_filter() -> str:
        parts = []
        if wallet:
            parts.append("wallet_id = :wallet")
            params["wallet"] = wallet
        if market_id:
            parts.append("market_id = :market_id")
            params["market_id"] = market_id
        if side:
            parts.append("side = :side")
            params["side"] = side.upper()
        return ("WHERE " + " AND ".join(parts)) if parts else ""

    w = build_filter()

    if table == "insider":
        union_sql = f"SELECT id, wallet_id, market_id, timestamp, side, outcome, prize, size_usdc, last_trade, 'insider' AS source FROM insiders_trades {w}"
    elif table == "normal":
        union_sql = f"SELECT id, wallet_id, market_id, timestamp, side, outcome, prize, size_usdc, last_trade, 'normal' AS source FROM trades {w}"
    else:
        union_sql = f"""
            SELECT id, wallet_id, market_id, timestamp, side, outcome, prize, size_usdc, last_trade, 'insider' AS source FROM insiders_trades {w}
            UNION ALL
            SELECT id, wallet_id, market_id, timestamp, side, outcome, prize, size_usdc, last_trade, 'normal' AS source FROM trades {w}
        """

    rows = (await session.execute(text(f"""
        SELECT t.*, m.question
        FROM ({union_sql}) t
        LEFT JOIN markets m ON m.market_id = t.market_id
        ORDER BY t.timestamp DESC
        LIMIT :limit OFFSET :offset
    """), params)).fetchall()

    count_sql = f"SELECT COUNT(*) FROM ({union_sql}) t"
    total = (await session.execute(text(count_sql), {k: v for k, v in params.items() if k not in ("limit", "offset")})).scalar()

    return {"items": [_row(r) for r in rows], "total": total or 0, "page": page}


def _row(r) -> dict:
    m = dict(r._mapping)
    if m.get("timestamp"):
        m["timestamp"] = m["timestamp"].isoformat()
    m["prize"] = float(m.get("prize") or 0)
    m["size_usdc"] = float(m.get("size_usdc") or 0)
    return m
