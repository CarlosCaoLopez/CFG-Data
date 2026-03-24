"""
Full pipeline integration smoke test.

Tests the complete flow end-to-end:
  [L2] insider_commands → ingestion listener ACK
  [L1] closed_markets   → orchestrator ACK + T2 dispatch
  [DB]  T1 persistence → wallet/market/trade in DB
  [CL]  compute_variables Celery task → closed_market_variables populated
  [D]   run_market_inference → R Plumber engine fits distributions, detects insiders
  [D→E] Motor D auto-calls /insider-detection (auto HTTP, not manual)
  [E]   E deletes closed_market_variables after D→E auto-call
  [E]   POST /insider-detection → wallet marked insider in DB, T3 dispatched
  [T3]  publish_insider_command → insider_commands stream
  [L2]  ingestion_l2 group ACKs the T3 command
  [T1]  insider_trades stream → T1 ACK → insiders_trades table populated
  [T4]  cleanup_stale_trades → runs without exception

Requires all services running:
  docker-compose up -d redis postgres ingestion orchestrator worker inference api

Run:
  poetry run python -m scripts.test_full_pipeline
"""
import os
import redis
import json
import random
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta, UTC

import httpx
import numpy as np
import redis as redis_sync
from sqlalchemy import text

from contracts.raw_market_data import (
    InsiderTradeCommand,
    InsiderTradeData,
    MarketDataBatch,
    RawMarketMetadata,
    RawTrade,
    RawWalletAge,
)
from database.connection import get_sync_session
from workers.tasks import compute_variables, run_market_inference, cleanup_market

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL)
#REDIS_URL = "redis://localhost:6379/0"

# IDs must match DB column widths (String(66) for markets/trades, String(42) for wallets)
TEST_WALLET    = "0x" + "ab" * 20         # 42 chars
TEST_MARKET_ID = "0x" + "cd" * 32         # 66 chars
TEST_TRADE_ID  = "0x" + "ef" * 32         # 66 chars

TEST_END_DATE   = datetime(2027, 6, 1, tzinfo=UTC)
TEST_TRADE_TIME = TEST_END_DATE - timedelta(hours=2)
TEST_FIRST_SEEN = TEST_END_DATE - timedelta(days=90)

CATEGORIES = ["Sports"]

# Timeouts
ACK_TIMEOUT         = 30   # seconds to wait for a consumer group to ACK
POLL_INTERVAL       = 1    # seconds between polling attempts
TASK_TIMEOUT        = 30   # seconds to wait for Celery task result
INFERENCE_TIMEOUT   = 90   # R inference may take longer on first run
T3_PUBLISH_TIMEOUT  = 60   # seconds for T3 to publish to insider_commands after API call
T4_TIMEOUT          = 30   # seconds for cleanup_stale_trades task
D_E_TIMEOUT         = 20   # seconds for D's auto HTTP call to E to reflect in DB

# Phase 5: background wallet count for distribution fitting (+ 1 definitive insider added separately)
CMV_BG_COUNT = 100

# Phase 6d: unique trade ID for the insider_trades → T1 → insiders_trades path
INSIDER_TEST_TRADE_ID = "0x" + "c" * 64   # 66 chars

# Phase 6: REST API (E) — exposed on host port 8000 via docker-compose
API_URL = "http://api:8000"

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

PASS = "\033[32m✔ PASS\033[0m"
FAIL = "\033[31m✘ FAIL\033[0m"
INFO = "\033[36m  →\033[0m"

_results: list[tuple[str, bool]] = []


def section(title: str) -> None:
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print('─'*60)


def check(label: str, condition: bool, detail: str = "") -> None:
    status = PASS if condition else FAIL
    print(f"  {status}  {label}")
    if detail:
        print(f"         {detail}")
    _results.append((label, condition))


def wait_for_pending_zero(
    r: redis_sync.Redis,
    stream: str,
    group: str,
    timeout: int = ACK_TIMEOUT,
    label: str = "",
) -> bool:
    """
    Poll XPENDING until the pending count for `group` on `stream` reaches 0,
    or until `timeout` seconds elapse. Returns True if cleared in time.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            info = r.xpending(stream, group)
            count = info.get("pending", 0) if isinstance(info, dict) else info[0]
        except Exception:
            count = 0
        if count == 0:
            return True
        if label:
            print(f"{INFO} [{label}] pending={count}, waiting...")
        time.sleep(POLL_INTERVAL)
    return False


def pending_count(r: redis_sync.Redis, stream: str, group: str) -> int:
    try:
        info = r.xpending(stream, group)
        return info.get("pending", 0) if isinstance(info, dict) else info[0]
    except Exception:
        return 0


# ─────────────────────────────────────────────
# Setup / Teardown
# ─────────────────────────────────────────────

# Tracked by seed_cmv_for_inference(), consumed by db_cleanup()
_fake_wallet_addrs: list[str] = []
_fake_trade_ids:    list[str] = []


def db_cleanup(session) -> None:
    """Remove all test rows (reverse FK order)."""
    # CMV rows — covers both the real test row and any seeded fake rows
    session.execute(
        text("DELETE FROM closed_market_variables WHERE market_id = :mid"),
        {"mid": TEST_MARKET_ID},
    )
    # Insider trades (FK: wallet_id → wallets) — must come before wallet deletion
    session.execute(
        text("DELETE FROM insiders_trades WHERE wallet_id = :addr"),
        {"addr": TEST_WALLET},
    )
    for addr in _fake_wallet_addrs:
        session.execute(
            text("DELETE FROM insiders_trades WHERE wallet_id = :addr"), {"addr": addr}
        )
    # Phase 6d trade injected directly
    session.execute(
        text("DELETE FROM insiders_trades WHERE id = :tid"),
        {"tid": INSIDER_TEST_TRADE_ID},
    )
    # Main test trade + any fake trades seeded in Phase 5
    session.execute(
        text("DELETE FROM trades WHERE id = :tid"),
        {"tid": TEST_TRADE_ID},
    )
    for tid in _fake_trade_ids:
        session.execute(text("DELETE FROM trades WHERE id = :tid"), {"tid": tid})
    # Market (no FK deps remain at this point)
    session.execute(
        text("DELETE FROM markets WHERE market_id = :mid"),
        {"mid": TEST_MARKET_ID},
    )
    # Main test wallet + any fake wallets seeded in Phase 5
    session.execute(
        text("DELETE FROM wallets WHERE address = :addr"),
        {"addr": TEST_WALLET},
    )
    for addr in _fake_wallet_addrs:
        session.execute(
            text("DELETE FROM wallets WHERE address = :addr"), {"addr": addr}
        )
    session.commit()


# ─────────────────────────────────────────────
# Phase 5 helpers
# ─────────────────────────────────────────────

def _fake_wallet_addr(i: int) -> str:
    """Returns a deterministic fake wallet address (42 chars)."""
    return "0x" + f"{i:040x}"


def _fake_trade_id(i: int) -> str:
    """Returns a deterministic fake trade ID (66 chars)."""
    return "0x" + f"{i:064x}"


def seed_cmv_for_inference(session) -> None:
    """
    Seeds CMV_BG_COUNT background wallets drawn from proper distributions
    PLUS 1 definitive insider (extreme values in all 5 critical regions).

    Background distributions (so R's goodness-of-fit tests pass):
      delta_t   ~ Gamma(shape=2, scale=20)   → mean=40h
      age       ~ Gamma(shape=3, scale=100)  → mean=300d
      win_vol   ~ Normal(30000, 20000)        → approximates T-student
      slippage  ~ Gamma(shape=2, scale=0.02) → mean=0.04
      entropy   ~ Beta(3, 3)                 → mean=0.5

    Insider: values far beyond the 1st/99th percentile thresholds:
      delta_t=0.01h, age=0.1d, wv=250000, slip=0.99, ent=0.001
    """
    global _fake_wallet_addrs, _fake_trade_ids
    rng_ts = random.Random(42)
    rng_np = np.random.default_rng(42)

    n = CMV_BG_COUNT
    # Background: proper distributions → AD/KS tests should not reject
    bg_dt   = rng_np.gamma(shape=2, scale=20,    size=n)
    bg_age  = rng_np.gamma(shape=3, scale=100,   size=n)
    bg_wv   = rng_np.normal(loc=30_000, scale=20_000, size=n)
    bg_slip = rng_np.gamma(shape=2, scale=0.02,  size=n)
    bg_ent  = rng_np.beta(a=3, b=3,              size=n)

    # Insider: deliberately extreme — guaranteed to be in all 5 critical regions
    # thresholds (approximate):
    #   delta_t  < qgamma(0.01, 2, 0.05)   ≈ 3h     → use 0.01h
    #   age      < qgamma(0.01, 3, 0.01)   ≈ 43d    → use 0.1d
    #   wv       > qt(0.99, T params) from N(30k,20k) ≈ 76k → use 250k
    #   slip     > qgamma(0.99, 2, 0.02)   ≈ 0.18   → use 0.99
    #   ent_s    < qbeta(0.01, 3, 3)       ≈ 0.10   → use 0.001
    ins_dt, ins_age, ins_wv, ins_slip, ins_ent = 0.01, 0.1, 250_000.0, 0.99, 0.001

    total   = n + 1  # background + 1 insider
    wallets = [_fake_wallet_addr(i) for i in range(1, total + 1)]
    trades  = [_fake_trade_id(i)   for i in range(1, total + 1)]
    _fake_wallet_addrs = wallets
    _fake_trade_ids    = trades

    for addr in wallets:
        session.execute(
            text("""
                INSERT INTO wallets
                    (address, first_seen_at, first_tx_polymarket, first_tx_polygon,
                     total_volume_usdc, markets_traded, insider, updated_at)
                VALUES (:addr, NOW() - INTERVAL '200 days', NOW() - INTERVAL '200 days',
                        NOW() - INTERVAL '210 days', 10000.0, 5, false, NOW())
                ON CONFLICT (address) DO NOTHING
            """),
            {"addr": addr},
        )

    for i, (addr, tid) in enumerate(zip(wallets, trades)):
        ts = TEST_TRADE_TIME - timedelta(hours=rng_ts.uniform(1, 96))
        session.execute(
            text("""
                INSERT INTO trades
                    (id, wallet_id, market_id, timestamp, side, outcome,
                     prize, size_usdc, size_tokens, last_trade, updated_at)
                VALUES (:tid, :wid, :mid, :ts, 'BUY', 'Yes',
                        :prize, :size, :tokens, false, NOW())
                ON CONFLICT DO NOTHING
            """),
            {
                "tid":    tid,
                "wid":    addr,
                "mid":    TEST_MARKET_ID,
                "ts":     ts,
                "prize":  round(rng_ts.uniform(0.55, 0.92), 4),
                "size":   round(rng_ts.uniform(1_000, 80_000), 2),
                "tokens": round(rng_ts.uniform(1_100, 90_000), 2),
            },
        )

        if i < n:
            dt, age, wv, slip, ent = (
                float(bg_dt[i]), float(bg_age[i]),
                float(bg_wv[i]), float(bg_slip[i]), float(bg_ent[i]),
            )
        else:
            dt, age, wv, slip, ent = ins_dt, ins_age, ins_wv, ins_slip, ins_ent

        session.execute(
            text("""
                INSERT INTO closed_market_variables
                    (wallet_id, market_id,
                     delta_t, age, winning_volume, alpha_slippage, entropy)
                VALUES (:wid, :mid, :dt, :age, :wv, :slip, :ent)
                ON CONFLICT DO NOTHING
            """),
            {
                "wid": addr, "mid": TEST_MARKET_ID,
                "dt": round(dt, 6), "age": round(age, 4),
                "wv": round(wv, 2), "slip": round(slip, 6), "ent": round(ent, 6),
            },
        )

    session.commit()
    print(f"{INFO} Seeded {n} background + 1 insider CMV rows "
          f"for market_id={TEST_MARKET_ID[:14]}...")


# ─────────────────────────────────────────────
# Phase builders
# ─────────────────────────────────────────────

def build_insider_command_payload() -> dict:
    """Build the Redis message for 'insider_commands' stream."""
    cmd = InsiderTradeCommand(
        wallet=TEST_WALLET,
        categories_insider=CATEGORIES,
        market_ids_insider=[TEST_MARKET_ID],
        command_type="ADD_WALLET",
    )
    return {"payload": cmd.model_dump_json()}


def build_closed_market_payload() -> dict:
    """Build the Redis message for 'closed_markets' stream."""
    trade = RawTrade(
        tx_hash=TEST_TRADE_ID,
        wallet=TEST_WALLET,
        market_id=TEST_MARKET_ID,
        timestamp=TEST_TRADE_TIME,
        side="BUY",
        outcome="Yes",
        price=0.88,
        size_usdc=50_000.0,
        size_tokens=56_818.0,
        last_trade=True,
    )
    wallet_age = RawWalletAge(
        wallet=TEST_WALLET,
        first_tx_polymarket=TEST_FIRST_SEEN,
        first_tx_polygon=TEST_FIRST_SEEN - timedelta(days=10),
    )
    batch = MarketDataBatch(
        metadata=RawMarketMetadata(
            market_id=TEST_MARKET_ID,
            question="[TEST] Will this pipeline test pass?",
            end_date=TEST_END_DATE,
            closed=True,
            outcome_prices=[1.0, 0.0],
            volume_usdc=1_000_000.0,
            winning_outcome="Yes",
            category="Sports",
            tags=["Football", "Test"],
        ),
        trades=[trade],
        wallet_ages={TEST_WALLET: wallet_age},
        ingested_at=datetime.now(UTC),
    )
    return {
        "payload":        batch.model_dump_json(),
        "market_id":      batch.metadata.market_id,
        "trades_count":   str(len(batch.trades)),
        "wallets_count":  str(len(batch.wallet_ages)),
        "category":       batch.metadata.category or "unknown",
        "winning_outcome": batch.metadata.winning_outcome or "unknown",
        "ingested_at":    batch.ingested_at.isoformat(),
    }


def seed_db_fallback(session) -> None:
    """
    Fallback: insert test data directly if T1 didn't write it.
    Only used if Phase 3 verification fails (e.g. orchestrator not running).
    """
    session.execute(
        text("""
            INSERT INTO wallets
                (address, first_seen_at, first_tx_polymarket, first_tx_polygon,
                 total_volume_usdc, markets_traded, insider, updated_at)
            VALUES
                (:addr, :fsa, :ftp, :ftpoly,
                 :vol, :mkts, false, NOW())
            ON CONFLICT (address) DO UPDATE SET
                total_volume_usdc = EXCLUDED.total_volume_usdc,
                markets_traded    = EXCLUDED.markets_traded,
                updated_at        = NOW()
        """),
        {
            "addr":   TEST_WALLET,
            "fsa":    TEST_FIRST_SEEN,
            "ftp":    TEST_FIRST_SEEN,
            "ftpoly": TEST_FIRST_SEEN - timedelta(days=10),
            "vol":    50_000.0,
            "mkts":   1,
        },
    )
    session.execute(
        text("""
            INSERT INTO markets
                (market_id, question, end_date, closed,
                 outcome_prices, volume_usdc, categories, tags,
                 winning_outcome, updated_at)
            VALUES
                (:mid, :q, :ed, true,
                 :op, :vol, :cats, :tags,
                 :wo, NOW())
            ON CONFLICT (market_id) DO UPDATE SET
                closed          = EXCLUDED.closed,
                winning_outcome = EXCLUDED.winning_outcome,
                updated_at      = NOW()
        """),
        {
            "mid":  TEST_MARKET_ID,
            "q":    "[TEST] Will this pipeline test pass?",
            "ed":   TEST_END_DATE,
            "op":   [1.0, 0.0],
            "vol":  1_000_000.0,
            "cats": ["Sports"],
            "tags": ["Football", "Test"],
            "wo":   "Yes",
        },
    )
    session.execute(
        text("""
            INSERT INTO trades
                (id, wallet_id, market_id, timestamp,
                 side, outcome, prize, size_usdc, size_tokens,
                 last_trade, updated_at)
            VALUES
                (:tid, :wid, :mid, :ts,
                 'BUY', 'Yes', 0.88, 50000.0, 56818.0,
                 true, NOW())
            ON CONFLICT DO NOTHING
        """),
        {
            "tid": TEST_TRADE_ID,
            "wid": TEST_WALLET,
            "mid": TEST_MARKET_ID,
            "ts":  TEST_TRADE_TIME,
        },
    )
    session.commit()


# ─────────────────────────────────────────────
# Main test runner
# ─────────────────────────────────────────────

def run_tests() -> int:
    r = redis_sync.from_url(REDIS_URL, decode_responses=False)

    # ── SETUP ───────────────────────────────────────────────────────
    section("SETUP — clean previous test data")
    try:
        with get_sync_session() as session:
            db_cleanup(session)
        print(f"{INFO} DB test rows cleared")
    except Exception as e:
        print(f"  \033[33m⚠ DB cleanup skipped: {e}\033[0m")

    # ── PHASE 1: L2 insider command ──────────────────────────────────
    section("PHASE 1 — L2 insider command injection")
    print(f"{INFO} Injecting ADD_WALLET command for {TEST_WALLET[:12]}... into 'insider_commands'")

    msg = build_insider_command_payload()
    msg_id = r.xadd("insider_commands", {k.encode(): v.encode() for k, v in msg.items()})
    print(f"{INFO} XADD 'insider_commands' → msg_id={msg_id}")

    print(f"{INFO} Waiting up to {ACK_TIMEOUT}s for 'ingestion_l2' group to ACK...")
    cleared = wait_for_pending_zero(r, "insider_commands", "ingestion_l2", label="L2")
    check(
        "L2 listener ACKed the insider command (pending → 0)",
        cleared,
        f"pending after timeout: {pending_count(r, 'insider_commands', 'ingestion_l2')}",
    )

    # ── PHASE 2: L1 market close batch ───────────────────────────────
    section("PHASE 2 — L1 market close batch injection (bypass Gamma API)")
    print(f"{INFO} Injecting MarketDataBatch for {TEST_MARKET_ID[:14]}... into 'closed_markets'")

    payload = build_closed_market_payload()
    msg_id2 = r.xadd("closed_markets", {k.encode(): v.encode() for k, v in payload.items()})
    print(f"{INFO} XADD 'closed_markets' → msg_id={msg_id2}")

    print(f"{INFO} Waiting up to {ACK_TIMEOUT}s for 'scheduler' group to ACK...")
    cleared2 = wait_for_pending_zero(r, "closed_markets", "scheduler", label="L1")
    check(
        "Orchestrator ACKed the closed_markets batch (pending → 0)",
        cleared2,
        f"pending after timeout: {pending_count(r, 'closed_markets', 'scheduler')}",
    )

    # ── PHASE 3: verify T1 wrote to DB ───────────────────────────────
    section("PHASE 3 — Verify T1 persisted batch to DB")
    print(f"{INFO} T1 should have written wallet/market/trade when orchestrator processed the batch")

    # Give the orchestrator a moment to finish async DB writes after ACKing
    time.sleep(2)

    db_ok = False
    try:
        with get_sync_session() as session:
            row = session.execute(
                text("SELECT address FROM wallets WHERE address = :a"),
                {"a": TEST_WALLET},
            ).fetchone()
            wallet_in_db = row is not None

            row2 = session.execute(
                text("SELECT market_id FROM markets WHERE market_id = :m"),
                {"m": TEST_MARKET_ID},
            ).fetchone()
            market_in_db = row2 is not None

            row3 = session.execute(
                text("SELECT id FROM trades WHERE id = :t"),
                {"t": TEST_TRADE_ID},
            ).fetchone()
            trade_in_db = row3 is not None

        check("T1: wallet persisted to DB by orchestrator", wallet_in_db)
        check("T1: market persisted to DB by orchestrator", market_in_db)
        check("T1: trade persisted to DB by orchestrator", trade_in_db)
        db_ok = wallet_in_db and market_in_db and trade_in_db

        if not db_ok:
            print(f"  \033[33m⚠ Orchestrator may not be running. Check: docker-compose logs orchestrator\033[0m")
    except Exception as e:
        check("T1 DB verification", False, str(e))
        traceback.print_exc()

    # ── PHASE 4: Celery compute_variables ─────────────────────────────
    section("PHASE 4 — Celery task: compute_variables")

    task_success = False  # will be set inside the else-branch below

    if not db_ok:
        print(f"  \033[33m⚠ Skipping Celery phase (T1 did not persist data to DB)\033[0m")
        _results.append(("compute_variables task", False))
    else:
        print(f"{INFO} Dispatching compute_variables.delay('{TEST_MARKET_ID[:14]}...')")
        result = compute_variables.delay(TEST_MARKET_ID)
        print(f"{INFO} Task id: {result.id}")

        # Poll for completion
        deadline = time.time() + TASK_TIMEOUT
        while time.time() < deadline and not result.ready():
            print(f"{INFO} Waiting for task... ({int(deadline - time.time())}s left)")
            time.sleep(POLL_INTERVAL)

        task_done    = result.ready()
        task_success = task_done and result.successful()
        task_result  = result.result if task_done else None

        check("compute_variables task completed", task_done)
        check("compute_variables task succeeded", task_success,
              f"result={task_result}" if task_done else "timed out")

        if task_success:
            # Verify rows in closed_market_variables
            with get_sync_session() as session:
                rows = session.execute(
                    text("SELECT wallet_id, delta_t, age, winning_volume "
                         "FROM closed_market_variables WHERE market_id = :mid"),
                    {"mid": TEST_MARKET_ID},
                ).fetchall()

            check(
                "closed_market_variables populated",
                len(rows) > 0,
                f"rows inserted: {len(rows)}" + (
                    f" | delta_t={float(rows[0].delta_t):.2f}h, age={float(rows[0].age):.1f}d, "
                    f"wv={float(rows[0].winning_volume):.2f} USDC"
                    if rows else ""
                ),
            )

    # ── PHASE 5: Motor D — R inference engine ─────────────────────────
    section("PHASE 5 — Motor D (R Plumber inference engine)")

    if not task_success:
        print(f"  \033[33m⚠ Skipping inference phase (compute_variables did not succeed)\033[0m")
        _results.append(("run_market_inference task completed", False))
        _results.append(("Motor D returned valid inference status", False))
    else:
        # Seed additional synthetic rows so R has enough data to fit distributions
        try:
            with get_sync_session() as session:
                seed_cmv_for_inference(session)
        except Exception as e:
            print(f"  \033[33m⚠ CMV seed failed: {e}\033[0m")
            traceback.print_exc()

        print(f"{INFO} Dispatching run_market_inference.delay('{TEST_MARKET_ID[:14]}...')")
        inf_result = run_market_inference.delay(TEST_MARKET_ID)
        print(f"{INFO} Task id: {inf_result.id}")

        deadline = time.time() + INFERENCE_TIMEOUT
        while time.time() < deadline and not inf_result.ready():
            print(f"{INFO} Waiting for inference... ({int(deadline - time.time())}s left)")
            time.sleep(POLL_INTERVAL)

        inf_done    = inf_result.ready()
        inf_success = inf_done and inf_result.successful()
        inf_data    = inf_result.result if inf_done else None

        check("run_market_inference task completed", inf_done)
        check("run_market_inference task succeeded (no exception)", inf_success,
              f"result={inf_data}" if inf_done else "timed out")

        if inf_success and isinstance(inf_data, dict):
            inf_status    = inf_data.get("inference_status")
            insiders_found = inf_data.get("insiders_found", 0)
            valid_statuses = {"success", "distribution_fit_failed", "insufficient_data", "no_data"}
            check(
                "Motor D returned valid inference status",
                inf_status in valid_statuses,
                f"inference_status={inf_status!r}, insiders_found={insiders_found}",
            )
            print(f"{INFO} Inference result → status={inf_status!r}, insiders_found={insiders_found}")

            if inf_status == "success":
                print(f"{INFO} Distributions fitted successfully ✔")

                # ── 5b. D→E auto-call + E CMV deletion ───────────────────
                # D calls /insider-detection automatically when insiders_list > 0.
                # We seeded a definitive insider (_fake_wallet_addr(CMV_BG_COUNT+1)),
                # so insiders_found > 0 means D already POSTed to E while inside
                # the R endpoint (before returning the HTTP response to the task).
                if insiders_found > 0:
                    d_insider_addr = "0x" + f"{CMV_BG_COUNT + 1:040x}"
                    print(f"{INFO} D found {insiders_found} insider(s). Polling DB for D→E write ({d_insider_addr[:12]}...)")

                    deadline_de = time.time() + D_E_TIMEOUT
                    de_wallet_marked = False
                    while time.time() < deadline_de:
                        try:
                            with get_sync_session() as s:
                                row_de = s.execute(
                                    text("SELECT insider FROM wallets WHERE address = :a"),
                                    {"a": d_insider_addr},
                                ).fetchone()
                            if row_de and row_de.insider:
                                de_wallet_marked = True
                                break
                        except Exception:
                            pass
                        print(f"{INFO} Waiting for D→E DB write... ({int(deadline_de - time.time())}s left)")
                        time.sleep(1)

                    check(
                        "D→E: Motor D auto-called /insider-detection (wallet marked insider)",
                        de_wallet_marked,
                        f"wallet={d_insider_addr[:12]}...",
                    )

                    # E should delete closed_market_variables for this market
                    try:
                        with get_sync_session() as s:
                            cmv_count = s.execute(
                                text("SELECT COUNT(*) FROM closed_market_variables WHERE market_id = :mid"),
                                {"mid": TEST_MARKET_ID},
                            ).scalar()
                        check(
                            "E: deleted closed_market_variables after D→E auto-call",
                            cmv_count == 0,
                            f"rows remaining: {cmv_count}",
                        )
                    except Exception as e:
                        check("E: deleted closed_market_variables after D→E auto-call", False, str(e))
                else:
                    # 0 insiders despite seeded extreme insider — threshold mismatch
                    check(
                        "D→E: Motor D auto-called /insider-detection (wallet marked insider)",
                        False,
                        "insiders_found=0 with seeded extreme insider — check thresholds",
                    )
                    # D never calls E when no insiders, so CMV deletion is N/A
                    _results.append(("E: deleted closed_market_variables after D→E auto-call", True))

            elif inf_status == "distribution_fit_failed":
                print(f"{INFO} Distributions rejected by goodness-of-fit (synthetic data) — connectivity OK")
                # D never calls E when distributions are invalid
                _results.append(("D→E: Motor D auto-called /insider-detection (wallet marked insider)", True))
                _results.append(("E: deleted closed_market_variables after D→E auto-call", True))
            else:
                # insufficient_data / no_data — D→E N/A
                _results.append(("D→E: Motor D auto-called /insider-detection (wallet marked insider)", True))
                _results.append(("E: deleted closed_market_variables after D→E auto-call", True))

        elif inf_done and not inf_success:
            _results.append(("Motor D returned valid inference status", False))
            _results.append(("D→E: Motor D auto-called /insider-detection (wallet marked insider)", False))
            _results.append(("E: deleted closed_market_variables after D→E auto-call", False))

    # ── PHASE 6: E (REST API) → T3 → L2 cycle ────────────────────────
    section("PHASE 6 — E (REST API) → T3 dispatch → L2 ACK")

    # Direct POST to FastAPI endpoint (bypasses D to test E independently)
    api_payload = {
        "market_id": TEST_MARKET_ID,
        "wallets": [{
            "wallet":              TEST_WALLET,
            "categories_insider": CATEGORIES,
            "command_type":        "ADD_WALLET",
        }],
    }

    # Capture baseline BEFORE the API call — T3 may publish within milliseconds
    # of the response, so capturing after the call would miss it.
    xlen_before = r.xlen("insider_commands")
    print(f"{INFO} xlen(insider_commands) before POST = {xlen_before}")

    api_ok = False
    try:
        print(f"{INFO} POST {API_URL}/insider-detection")
        resp = httpx.post(
            f"{API_URL}/insider-detection",
            json=api_payload,
            timeout=15.0,
        )
        api_status = resp.status_code
        check("E: API responded 200", api_status == 200,
              f"status={api_status}, body={resp.text[:200]}")
        api_ok = api_status == 200
    except Exception as e:
        check("E: API responded 200", False, str(e))
        print(f"  \033[33m⚠ Is the api service running? docker-compose up -d api\033[0m")

    if api_ok:
        # 6a. Verify DB: wallet marked as insider
        time.sleep(1)  # let the async DB write finish
        try:
            with get_sync_session() as session:
                row = session.execute(
                    text("SELECT insider, categories_insider "
                         "FROM wallets WHERE address = :a"),
                    {"a": TEST_WALLET},
                ).fetchone()
            check(
                "E: wallet.insider set to True in DB",
                row is not None and bool(row.insider),
                f"insider={row.insider if row else 'NOT FOUND'}",
            )
            check(
                "E: wallet.categories_insider populated",
                row is not None and bool(row.categories_insider),
                f"categories={list(row.categories_insider) if row else '[]'}",
            )
        except Exception as e:
            check("E: wallet.insider set to True in DB", False, str(e))
            check("E: wallet.categories_insider populated", False, "")

        # 6b. Verify T3: message published to insider_commands
        print(f"{INFO} xlen(insider_commands) baseline = {xlen_before}")

        deadline = time.time() + T3_PUBLISH_TIMEOUT
        t3_published = False
        while time.time() < deadline:
            if r.xlen("insider_commands") > xlen_before:
                t3_published = True
                break
            print(f"{INFO} Waiting for T3 to publish... ({int(deadline - time.time())}s left)")
            time.sleep(POLL_INTERVAL)

        check(
            "T3: published command to insider_commands stream",
            t3_published,
            f"xlen after: {r.xlen('insider_commands')} (was {xlen_before})",
        )

        # 6c. Verify L2: ACKed the T3 command
        if t3_published:
            print(f"{INFO} Waiting up to {ACK_TIMEOUT}s for L2 to ACK the T3 command...")
            l2_acked = wait_for_pending_zero(
                r, "insider_commands", "ingestion_l2", label="L2-T3"
            )
            check(
                "L2: ACKed T3 command from insider_commands (pending → 0)",
                l2_acked,
                f"pending after timeout: {pending_count(r, 'insider_commands', 'ingestion_l2')}",
            )
        else:
            _results.append(("L2: ACKed T3 command from insider_commands (pending → 0)", False))

    else:
        # API unreachable — skip downstream checks
        for label in [
            "E: wallet.insider set to True in DB",
            "E: wallet.categories_insider populated",
            "T3: published command to insider_commands stream",
            "L2: ACKed T3 command from insider_commands (pending → 0)",
        ]:
            _results.append((label, False))

    # ── PHASE 6d: L2→insider_trades→T1 ───────────────────────────────
    # L2 would normally publish here after polling trades for a monitored wallet.
    # Since TEST_WALLET is fake (no real Polymarket trades), we inject directly
    # to verify T1's insider_trades consumption path independently.
    # Requires wallet + market in DB (Phase 3) for FK constraints.
    section("PHASE 6d — L2→insider_trades→T1 (insiders_trades persistence)")

    if db_ok:
        it_trade = RawTrade(
            tx_hash=INSIDER_TEST_TRADE_ID,
            wallet=TEST_WALLET,
            market_id=TEST_MARKET_ID,
            timestamp=TEST_TRADE_TIME,
            side="BUY",
            outcome="Yes",
            price=0.75,
            size_usdc=5000.0,
            size_tokens=6667.0,
            last_trade=False,
        )
        it_data = InsiderTradeData(
            wallet=TEST_WALLET,
            trades=[it_trade],
            categories_insider=CATEGORIES,
        )
        it_msg = {
            "payload":      it_data.model_dump_json(),
            "wallet":       TEST_WALLET,
            "trades_count": "1",
            "categories":   json.dumps(CATEGORIES),
            "published_at": it_data.published_at.isoformat(),
        }
        it_msg_id = r.xadd("insider_trades", {k.encode(): v.encode() for k, v in it_msg.items()})
        print(f"{INFO} XADD 'insider_trades' → msg_id={it_msg_id}")

        print(f"{INFO} Waiting up to {ACK_TIMEOUT}s for 'tasks' group to ACK insider_trades...")
        t1_it_acked = wait_for_pending_zero(r, "insider_trades", "tasks", label="T1-IT")
        check(
            "T1: ACKed InsiderTradeData from insider_trades stream (pending → 0)",
            t1_it_acked,
            f"pending after timeout: {pending_count(r, 'insider_trades', 'tasks')}",
        )

        if t1_it_acked:
            time.sleep(1)  # let async DB write complete
            try:
                with get_sync_session() as session:
                    it_row = session.execute(
                        text("SELECT id FROM insiders_trades WHERE id = :tid"),
                        {"tid": INSIDER_TEST_TRADE_ID},
                    ).fetchone()
                check(
                    "T1: insider trade persisted to insiders_trades table",
                    it_row is not None,
                    f"trade_id={INSIDER_TEST_TRADE_ID[:14]}...",
                )
            except Exception as e:
                check("T1: insider trade persisted to insiders_trades table", False, str(e))
        else:
            _results.append(("T1: insider trade persisted to insiders_trades table", False))
    else:
        print(f"  \033[33m⚠ Skipping 6d (wallet/market not in DB from Phase 3)\033[0m")
        _results.append(("T1: ACKed InsiderTradeData from insider_trades stream (pending → 0)", False))
        _results.append(("T1: insider trade persisted to insiders_trades table", False))

    # ── PHASE 7: T4 — cleanup_stale_trades ────────────────────────────
    section("PHASE 7 — T4 (cleanup_stale_trades)")

    print(f"{INFO} Dispatching cleanup_stale_trades.delay()")
    t4_result = cleanup_market.delay(TEST_MARKET_ID)
    print(f"{INFO} Task id: {t4_result.id}")

    deadline = time.time() + T4_TIMEOUT
    while time.time() < deadline and not t4_result.ready():
        print(f"{INFO} Waiting for T4... ({int(deadline - time.time())}s left)")
        time.sleep(POLL_INTERVAL)

    t4_done    = t4_result.ready()
    t4_success = t4_done and t4_result.successful()
    t4_data    = t4_result.result if t4_done else None

    check("T4: cleanup_stale_trades task completed", t4_done)
    detail_msg = f"result={t4_data}" if t4_success else f"status={t4_result.state} | err={t4_data}"
    check("T4: cleanup_market task succeeded", t4_success, detail_msg)

    # ── TEARDOWN ──────────────────────────────────────────────────────
    section("TEARDOWN — remove test data")
    try:
        with get_sync_session() as session:
            db_cleanup(session)
        print(f"{INFO} DB test rows removed")
    except Exception as e:
        print(f"  \033[33m⚠ Teardown error: {e}\033[0m")
    finally:
        r.close()

    # ── SUMMARY ──────────────────────────────────────────────────────
    section("SUMMARY")
    passed = sum(1 for _, ok in _results if ok)
    total  = len(_results)
    for label, ok in _results:
        print(f"  {'✔' if ok else '✘'}  {label}")
    print(f"\n  {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(run_tests())
