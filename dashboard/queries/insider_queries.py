from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text


async def get_insiders(
    session: AsyncSession,
    page: int = 1,
    limit: int = 50,
    category: str | None = None,
) -> dict:
    offset = (page - 1) * limit
    params: dict = {"limit": limit, "offset": offset}
    cat_filter = ""
    if category:
        cat_filter = "AND :category = ANY(w.categories_insider)"
        params["category"] = category

    rows = (await session.execute(text(f"""
        SELECT
          w.address, w.insider, w.categories_insider, w.marketids_insider,
          w.total_volume_usdc, w.markets_traded,
          w.first_tx_polygon, w.first_tx_polymarket, w.updated_at,
          COUNT(it.id) AS insider_trades_count
        FROM wallets w
        LEFT JOIN insiders_trades it ON it.wallet_id = w.address
        WHERE w.insider = TRUE {cat_filter}
        GROUP BY w.address
        ORDER BY w.updated_at DESC
        LIMIT :limit OFFSET :offset
    """), params)).fetchall()

    total_params = {k: v for k, v in params.items() if k not in ("limit", "offset")}
    total = (await session.execute(text(f"""
        SELECT COUNT(*) FROM wallets w WHERE w.insider = TRUE {cat_filter}
    """), total_params)).scalar()

    return {"items": [_wallet_row(r) for r in rows], "total": total or 0, "page": page}


async def get_insider_detail(session: AsyncSession, wallet: str) -> dict | None:
    row = (await session.execute(text("""
        SELECT w.*, COUNT(it.id) AS insider_trades_count
        FROM wallets w
        LEFT JOIN insiders_trades it ON it.wallet_id = w.address
        WHERE w.address = :wallet
        GROUP BY w.address
    """), {"wallet": wallet})).fetchone()
    if not row:
        return None

    trades = (await session.execute(text("""
        SELECT it.id, it.market_id, m.question, it.timestamp,
               it.side, it.outcome, it.prize, it.size_usdc, it.last_trade
        FROM insiders_trades it
        JOIN markets m ON m.market_id = it.market_id
        WHERE it.wallet_id = :wallet
        ORDER BY it.timestamp DESC LIMIT 200
    """), {"wallet": wallet})).fetchall()

    result = _wallet_row(row)
    result["insider_trades"] = [_trade_row(t) for t in trades]
    return result


def _wallet_row(r) -> dict:
    m = dict(r._mapping)
    for field in ("first_tx_polygon", "first_tx_polymarket", "updated_at"):
        if m.get(field):
            m[field] = m[field].isoformat()
    m["total_volume_usdc"] = float(m.get("total_volume_usdc") or 0)
    m["insider_trades_count"] = int(m.get("insider_trades_count") or 0)
    return m


def _trade_row(r) -> dict:
    m = dict(r._mapping)
    if m.get("timestamp"):
        m["timestamp"] = m["timestamp"].isoformat()
    m["prize"] = float(m.get("prize") or 0)
    m["size_usdc"] = float(m.get("size_usdc") or 0)
    return m
