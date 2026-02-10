"""
Test de compute_variables:
  1. Inserta datos de ejemplo (markets, wallets, trades)
  2. Ejecuta la task compute_variables para un market
  3. Verifica que closed_market_variables tiene los resultados esperados
"""
import asyncio
import math
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timedelta, UTC

from sqlalchemy import text
from database.connection import async_session
from database.manager import DatabaseManager

_SQL_DIR = Path(__file__).resolve().parent / "scripts" / "sql"

# ── Constantes de test ────────────────────────────────────────────────────────
TARGET_MARKET_ID = "0xTEST_MARKET_A"
EXTRA_MARKET_ID  = "0xTEST_MARKET_B"

WALLET_1 = "0xWALLET_0001_MULTI_TRADE_AAA"  # 2 trades en A, 1 en B
WALLET_2 = "0xWALLET_0002_SINGLE_TRADE_BB"  # 1 trade en A
WALLET_3 = "0xWALLET_0003_HEAVY_TRADER_CC"  # 3 trades en A, 1 en B

# Fechas base
MARKET_END = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
W1_FIRST_SEEN = MARKET_END - timedelta(days=30)
W2_FIRST_SEEN = MARKET_END - timedelta(days=2)   # wallet reciente
W3_FIRST_SEEN = MARKET_END - timedelta(days=365)  # wallet veterana

MARKET_A_VOLUME = Decimal("500000.00")
MARKET_B_VOLUME = Decimal("200000.00")


async def cleanup():
    """Limpia datos de test previos."""
    async with async_session() as session:
        # Orden importa por FKs
        await session.execute(text(
            "DELETE FROM closed_market_variables WHERE market_id IN (:a, :b)"
        ), {"a": TARGET_MARKET_ID, "b": EXTRA_MARKET_ID})
        await session.execute(text(
            "DELETE FROM trades WHERE market_id IN (:a, :b)"
        ), {"a": TARGET_MARKET_ID, "b": EXTRA_MARKET_ID})
        await session.execute(text(
            "DELETE FROM markets WHERE market_id IN (:a, :b)"
        ), {"a": TARGET_MARKET_ID, "b": EXTRA_MARKET_ID})
        await session.execute(text(
            "DELETE FROM wallets WHERE address IN (:w1, :w2, :w3)"
        ), {"w1": WALLET_1, "w2": WALLET_2, "w3": WALLET_3})
        await session.commit()
    print("[OK] Datos de test previos limpiados")


async def seed_data():
    """Inserta markets, wallets y trades de prueba."""
    async with async_session() as session:
        db = DatabaseManager(session)

        # ── Markets ───────────────────────────────────────────────────────
        await db.upsert_market({
            "market_id": TARGET_MARKET_ID,
            "question": "Test: Will BTC reach 200k?",
            "end_date": MARKET_END,
            "closed": True,
            "outcome_prices": [0.70, 0.30],
            "volume_usdc": MARKET_A_VOLUME,
            "categories": ["crypto", "finance"],
            "winning_outcome": "Yes",
        })
        await db.upsert_market({
            "market_id": EXTRA_MARKET_ID,
            "question": "Test: Will Team X win?",
            "end_date": MARKET_END + timedelta(days=10),
            "closed": True,
            "outcome_prices": [0.50, 0.50],
            "volume_usdc": MARKET_B_VOLUME,
            "categories": ["sports"],
            "winning_outcome": "No",
        })
        print("[OK] Markets insertados")

        # ── Wallets ───────────────────────────────────────────────────────
        for addr, first_seen in [
            (WALLET_1, W1_FIRST_SEEN),
            (WALLET_2, W2_FIRST_SEEN),
            (WALLET_3, W3_FIRST_SEEN),
        ]:
            await db.upsert_wallet({
                "address": addr,
                "first_seen_at": first_seen,
                "total_volume_usdc": Decimal("10000.00"),
                "markets_traded": 2,
            })
        print("[OK] Wallets insertadas")

        # ── Trades ────────────────────────────────────────────────────────
        trades = [
            # Wallet 1: 2 trades en Market A (1 win, 1 lose) + 1 en Market B
            {
                "id": "0xTRADE_W1_A1",
                "wallet_id": WALLET_1,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=6),
                "side": "BUY", "outcome": "Yes",       # win
                "prize": Decimal("0.70"),
                "size_usdc": Decimal("2000.00"),
                "size_tokens": Decimal("2857.14"),
            },
            {
                "id": "0xTRADE_W1_A2",
                "wallet_id": WALLET_1,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=2),
                "side": "BUY", "outcome": "No",        # lose
                "prize": Decimal("0.30"),
                "size_usdc": Decimal("500.00"),
                "size_tokens": Decimal("1666.67"),
            },
            {
                "id": "0xTRADE_W1_B1",
                "wallet_id": WALLET_1,
                "market_id": EXTRA_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=10),
                "side": "BUY", "outcome": "No",
                "prize": Decimal("0.50"),
                "size_usdc": Decimal("1000.00"),
                "size_tokens": Decimal("2000.00"),
            },
            # Wallet 2: 1 trade en Market A
            {
                "id": "0xTRADE_W2_A1",
                "wallet_id": WALLET_2,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=1),
                "side": "BUY", "outcome": "Yes",       # win
                "prize": Decimal("0.90"),
                "size_usdc": Decimal("10000.00"),
                "size_tokens": Decimal("11111.11"),
            },
            # Wallet 3: 3 trades en Market A + 1 en Market B
            {
                "id": "0xTRADE_W3_A1",
                "wallet_id": WALLET_3,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=48),
                "side": "BUY", "outcome": "Yes",       # win
                "prize": Decimal("0.50"),
                "size_usdc": Decimal("5000.00"),
                "size_tokens": Decimal("10000.00"),
            },
            {
                "id": "0xTRADE_W3_A2",
                "wallet_id": WALLET_3,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=24),
                "side": "BUY", "outcome": "Yes",       # win
                "prize": Decimal("0.60"),
                "size_usdc": Decimal("3000.00"),
                "size_tokens": Decimal("5000.00"),
            },
            {
                "id": "0xTRADE_W3_A3",
                "wallet_id": WALLET_3,
                "market_id": TARGET_MARKET_ID,
                "timestamp": MARKET_END - timedelta(hours=3),
                "side": "BUY", "outcome": "No",        # lose
                "prize": Decimal("0.40"),
                "size_usdc": Decimal("1000.00"),
                "size_tokens": Decimal("2500.00"),
            },
            {
                "id": "0xTRADE_W3_B1",
                "wallet_id": WALLET_3,
                "market_id": EXTRA_MARKET_ID,
                "timestamp": MARKET_END + timedelta(days=5),
                "side": "BUY", "outcome": "Yes",
                "prize": Decimal("0.55"),
                "size_usdc": Decimal("2000.00"),
                "size_tokens": Decimal("3636.36"),
            },
        ]
        for t in trades:
            await db.add_trade(t)
        print(f"[OK] {len(trades)} trades insertados")


async def verify_results():
    """Lee closed_market_variables y valida los resultados."""
    async with async_session() as session:
        rows = (await session.execute(text(
            "SELECT wallet_id, trade_id, delta_t, age, winning_volume, alpha_slippage, entropy "
            "FROM closed_market_variables "
            "WHERE market_id = :mid "
            "ORDER BY wallet_id, trade_id"
        ), {"mid": TARGET_MARKET_ID})).fetchall()

    if not rows:
        print("[FAIL] No se encontraron filas en closed_market_variables")
        return False

    print(f"\n{'='*100}")
    print(f"  RESULTADOS: {len(rows)} filas en closed_market_variables para market {TARGET_MARKET_ID}")
    print(f"{'='*100}")
    print(f"  {'wallet_id':<35} {'trade_id':<18} {'delta_t':>10} {'age':>10} {'win_vol':>14} {'slippage':>12} {'entropy':>10}")
    print(f"  {'-'*35} {'-'*18} {'-'*10} {'-'*10} {'-'*14} {'-'*12} {'-'*10}")

    all_ok = True
    for r in rows:
        wallet_id, trade_id, delta_t, age, winning_volume, alpha_slippage, entropy = r
        wallet_short = wallet_id[:20] + "..."
        trade_short = trade_id[:16] + ".."
        print(f"  {wallet_short:<35} {trade_short:<18} {float(delta_t):>10.2f}h {float(age):>9.1f}d {float(winning_volume):>13.2f} {float(alpha_slippage):>11.6f} {float(entropy):>10.4f}")

        # Validaciones basicas
        if delta_t <= 0:
            print(f"    [WARN] delta_t <= 0 para {trade_id}")
            all_ok = False
        if age < 1:
            print(f"    [FAIL] age < 1 para {wallet_id}")
            all_ok = False
        if alpha_slippage < 0:
            print(f"    [FAIL] alpha_slippage < 0 para {trade_id}")
            all_ok = False
        if entropy < 0:
            print(f"    [FAIL] entropy < 0 para {wallet_id}")
            all_ok = False

    # ── Validaciones especificas ──────────────────────────────────────────

    rows_by_trade = {r[1]: r for r in rows}

    # Wallet 1, Trade A1: delta_t = 6h, outcome=Yes (win)
    r = rows_by_trade.get("0xTRADE_W1_A1")
    if r:
        assert abs(float(r[2]) - 6.0) < 0.01, f"delta_t W1_A1 esperado ~6h, obtenido {r[2]}"
        # age = 30 dias
        assert abs(float(r[3]) - 30.0) < 0.1, f"age W1 esperado ~30d, obtenido {r[3]}"
        # winning_volume W1: 2000*0.70*(+1) + 500*0.30*(-1) = 1400 - 150 = 1250
        assert abs(float(r[4]) - 1250.0) < 0.01, f"winning_vol W1 esperado 1250, obtenido {r[4]}"
        # slippage: 2000 / 500000 = 0.004
        assert abs(float(r[5]) - 0.004) < 0.0001, f"slippage W1_A1 esperado 0.004, obtenido {r[5]}"
        print("  [CHECK] Wallet1 Trade A1: delta_t, age, winning_vol, slippage OK")

    # Wallet 2, Trade A1: delta_t = 1h, age = 2d, outcome=Yes (win)
    r = rows_by_trade.get("0xTRADE_W2_A1")
    if r:
        assert abs(float(r[2]) - 1.0) < 0.01, f"delta_t W2_A1 esperado ~1h, obtenido {r[2]}"
        assert abs(float(r[3]) - 2.0) < 0.1, f"age W2 esperado ~2d, obtenido {r[3]}"
        # winning_volume W2: 10000*0.90*(+1) = 9000
        assert abs(float(r[4]) - 9000.0) < 0.01, f"winning_vol W2 esperado 9000, obtenido {r[4]}"
        # slippage: 10000 / 500000 = 0.02
        assert abs(float(r[5]) - 0.02) < 0.0001, f"slippage W2_A1 esperado 0.02, obtenido {r[5]}"
        # W2 solo opera en Market A (categories: crypto, finance) -> entropia = ln(1) = 0
        # Porque todo su volumen esta en las mismas 2 categorias de un solo market
        # Espera: P(crypto) = 0.5, P(finance) = 0.5 -> H = -2*(0.5*ln(0.5)) = ln(2) ~ 0.6931
        expected_entropy = math.log(2)
        assert abs(float(r[6]) - expected_entropy) < 0.01, f"entropy W2 esperado {expected_entropy:.4f}, obtenido {r[6]}"
        print("  [CHECK] Wallet2 Trade A1: delta_t, age, winning_vol, slippage, entropy OK")

    # Wallet 3: 3 trades en A, 1 en B -> entropia con 3 categorias
    r = rows_by_trade.get("0xTRADE_W3_A1")
    if r:
        assert abs(float(r[2]) - 48.0) < 0.01, f"delta_t W3_A1 esperado ~48h, obtenido {r[2]}"
        assert abs(float(r[3]) - 365.0) < 0.5, f"age W3 esperado ~365d, obtenido {r[3]}"
        # winning_volume W3: 5000*0.50*(+1) + 3000*0.60*(+1) + 1000*0.40*(-1) = 2500 + 1800 - 400 = 3900
        assert abs(float(r[4]) - 3900.0) < 0.01, f"winning_vol W3 esperado 3900, obtenido {r[4]}"
        # W3 tiene trades en crypto(9000), finance(9000), sports(2000)
        # total = 20000
        # P(crypto)=9000/20000=0.45, P(finance)=0.45, P(sports)=0.10
        # H = -(0.45*ln(0.45) + 0.45*ln(0.45) + 0.10*ln(0.10))
        p_crypto = 9000 / 20000
        p_finance = 9000 / 20000
        p_sports = 2000 / 20000
        expected_h = -(p_crypto * math.log(p_crypto) + p_finance * math.log(p_finance) + p_sports * math.log(p_sports))
        assert abs(float(r[6]) - expected_h) < 0.01, f"entropy W3 esperado {expected_h:.4f}, obtenido {r[6]}"
        print(f"  [CHECK] Wallet3 Trade A1: delta_t, age, winning_vol, entropy({expected_h:.4f}) OK")

    # Verificar que trades de Wallet 1 comparten el mismo winning_volume
    w1_trades = [r for r in rows if r[0] == WALLET_1]
    if len(w1_trades) == 2:
        assert w1_trades[0][4] == w1_trades[1][4], "winning_volume debe ser igual para ambos trades de W1"
        print("  [CHECK] Wallet1: winning_volume consistente entre trades OK")

    # Verificar conteo total de filas (6 trades en Market A)
    assert len(rows) == 6, f"Esperadas 6 filas, obtenidas {len(rows)}"
    print(f"  [CHECK] Conteo total de filas: {len(rows)} OK")

    print(f"\n{'='*100}")
    if all_ok:
        print("  TODOS LOS CHECKS PASARON")
    else:
        print("  ALGUNOS CHECKS FALLARON (ver warnings arriba)")
    print(f"{'='*100}\n")

    return all_ok


async def run_compute_variables_sql():
    """Ejecuta el mismo SQL que la task Celery compute_variables, dentro del loop async."""
    sql_text = (_SQL_DIR / "compute_variables.sql").read_text(encoding="utf-8")
    async with async_session() as session:
        result = await session.execute(text(sql_text), {"market_id": TARGET_MARKET_ID})
        rows_affected = result.rowcount
        await session.commit()
    print(f"[OK] SQL compute_variables ejecutado: {rows_affected} filas afectadas")
    return rows_affected


async def main():
    await cleanup()
    await seed_data()
    await run_compute_variables_sql()
    ok = await verify_results()
    # await cleanup()
    # print("[OK] Cleanup final completado")
    return ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    exit(0 if ok else 1)
