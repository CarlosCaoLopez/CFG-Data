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


async def get_insider_detail(
    session: AsyncSession,
    wallet: str,
    limit: int = 500,
    offset: int = 0,
) -> dict | None:
    row = (await session.execute(text("""
        SELECT w.*, COUNT(it.id) AS insider_trades_count
        FROM wallets w
        LEFT JOIN insiders_trades it ON it.wallet_id = w.address
        WHERE w.address = :wallet
        GROUP BY w.address
    """), {"wallet": wallet})).fetchone()
    if not row:
        return None

    result = _wallet_row(row)
    detection_market_ids = set(result.get("marketids_insider") or [])

    trades = (await session.execute(text("""
        SELECT it.id, it.market_id, m.question, m.winning_outcome, m.closed, m.closed_time,
               it.timestamp, it.side, it.outcome, it.prize, it.size_usdc,
               it.last_trade, it.is_detection_trade
        FROM insiders_trades it
        JOIN markets m ON m.market_id = it.market_id
        WHERE it.wallet_id = :wallet
        ORDER BY it.timestamp DESC
        LIMIT :limit OFFSET :offset
    """), {"wallet": wallet, "limit": limit, "offset": offset})).fetchall()

    detections = (await session.execute(text("""
        SELECT d.market_id, m.question, d.categories_insider, d.detected_at, d.status
        FROM insider_detections d
        JOIN markets m ON m.market_id = d.market_id
        WHERE d.wallet = :wallet
        ORDER BY d.detected_at DESC
    """), {"wallet": wallet})).fetchall()

    result["detections"] = [_detection_row(d) for d in detections]
    result["markets"], result["pnl_total"], result["hit_rate_total"] = _group_trades_by_market(
        trades, detection_market_ids
    )
    return result


def _group_trades_by_market(trades, detection_market_ids: set[str]) -> tuple[list[dict], float, float | None]:
    """
    Groups trades by market_id (preserving order of first appearance — trades
    arrive sorted by timestamp DESC, so most recently active market first).
    Computes per-trade and per-market PnL (winning_volume formula from the
    architecture: +size_usdc if outcome==winning_outcome, -size_usdc otherwise;
    None while the market is still open / winning_outcome unknown).
    """
    groups: dict[str, dict] = {}
    order: list[str] = []

    total_pnl = 0.0
    total_wins = 0
    total_with_outcome = 0

    for t in trades:
        m = dict(t._mapping)
        market_id = m["market_id"]
        if market_id not in groups:
            order.append(market_id)
            groups[market_id] = {
                "market_id": market_id,
                "question": m["question"],
                "winning_outcome": m["winning_outcome"],
                "closed": m["closed"],
                "closed_time": m["closed_time"].isoformat() if m["closed_time"] else None,
                "is_detection_market": market_id in detection_market_ids,
                "trades": [],
                "trades_count": 0,
                "volume_usdc": 0.0,
                "pnl": 0.0,
                "_wins": 0,
                "_with_outcome": 0,
            }

        size_usdc = float(m["size_usdc"] or 0)
        pnl = None
        if m["winning_outcome"]:
            won = m["outcome"] == m["winning_outcome"]
            pnl = size_usdc if won else -size_usdc
            groups[market_id]["_with_outcome"] += 1
            total_with_outcome += 1
            if won:
                groups[market_id]["_wins"] += 1
                total_wins += 1
            groups[market_id]["pnl"] += pnl
            total_pnl += pnl

        groups[market_id]["trades_count"] += 1
        groups[market_id]["volume_usdc"] += size_usdc
        groups[market_id]["trades"].append({
            "id": m["id"],
            "timestamp": m["timestamp"].isoformat() if m["timestamp"] else None,
            "side": m["side"],
            "outcome": m["outcome"],
            "prize": float(m["prize"] or 0),
            "size_usdc": size_usdc,
            "last_trade": m["last_trade"],
            "is_detection_trade": m["is_detection_trade"],
            "pnl": pnl,
        })

    markets = []
    for market_id in order:
        g = groups[market_id]
        with_outcome = g.pop("_with_outcome")
        wins = g.pop("_wins")
        g["hit_rate"] = (wins / with_outcome) if with_outcome else None
        markets.append(g)

    hit_rate_total = (total_wins / total_with_outcome) if total_with_outcome else None
    return markets, total_pnl, hit_rate_total


def _wallet_row(r) -> dict:
    m = dict(r._mapping)
    for field in ("first_tx_polygon", "first_tx_polymarket", "updated_at"):
        if m.get(field):
            m[field] = m[field].isoformat()
    m["total_volume_usdc"] = float(m.get("total_volume_usdc") or 0)
    m["insider_trades_count"] = int(m.get("insider_trades_count") or 0)
    return m


def _detection_row(r) -> dict:
    m = dict(r._mapping)
    if m.get("detected_at"):
        m["detected_at"] = m["detected_at"].isoformat()
    return m
