"""
One-shot seed: re-arm L2 monitoring for insiders already in the DB.

Background: L2's monitored-wallet set is mirrored to the Redis hash
`monitored:wallets` and rehydrated at startup (see
InsiderTradesListener._load_monitored_wallets). But wallets detected BEFORE
that mirror existed were never written to it, so after a redeploy L2 stops
tracking them silently ("Seguimiento (L2): Inactivo", no insider trades
collected, 0 markets in the dashboard).

This script reads `wallets WHERE insider = TRUE` and, for any insider not
already present in `monitored:wallets`, publishes an ADD_WALLET command to
the `insider_commands` stream — the exact same path E/T3 use. L2 consumes it
live via its command listener and persists it to the hash. Two properties
this design guarantees:

  * No race with L2 startup: the command waits in the durable stream and is
    delivered whenever L2's consumer group reads `>`, up or not.
  * No last_seen_ts clobbering: wallets already in the hash are skipped, so a
    re-run never resets the polling cursor of an actively-tracked wallet.

Categories come from `wallets.categories_insider` (per-wallet, set at
detection), NOT the global CATEGORIAS_DE_INTERES env — so an insider flagged
in a category later removed from the env keeps being monitored in it.

Idempotent: after the first successful run the wallet is in the hash, so
subsequent runs skip it. Best-effort: never fails the caller (always exit 0),
so it can be chained before the orchestrator boots without blocking it.

Run standalone:
    poetry run python -m scripts.seed_monitored_wallets
Wired into orchestrator startup (see docker-compose.yml).
"""
import os
import sys

import redis as redis_sync
from sqlalchemy import text

from database.connection import get_sync_session
from contracts.raw_market_data import InsiderTradeCommand

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MONITORED_WALLETS_KEY = "monitored:wallets"
COMMANDS_STREAM = "insider_commands"


def seed() -> int:
    try:
        with get_sync_session() as session:
            rows = session.execute(
                text(
                    "SELECT address, categories_insider, marketids_insider "
                    "FROM wallets WHERE insider = TRUE"
                )
            ).fetchall()
    except Exception as e:
        print(f"[seed_monitored_wallets] DB read failed: {e}", file=sys.stderr)
        return 0

    if not rows:
        print("[seed_monitored_wallets] no insider wallets in DB, nothing to seed")
        return 0

    seeded = 0
    skipped = 0
    r = redis_sync.from_url(REDIS_URL)
    try:
        for row in rows:
            wallet = (row.address or "").lower()
            if not wallet:
                continue

            # Skip wallets already mirrored — preserves their last_seen_ts.
            try:
                if r.hexists(MONITORED_WALLETS_KEY, wallet):
                    skipped += 1
                    continue
            except Exception as e:
                print(f"[seed_monitored_wallets] HEXISTS failed for {wallet}: {e}", file=sys.stderr)
                continue

            cmd = InsiderTradeCommand(
                wallet=wallet,
                categories_insider=list(row.categories_insider or []),
                market_ids_insider=list(row.marketids_insider or []) or None,
                command_type="ADD_WALLET",
            )
            try:
                r.xadd(COMMANDS_STREAM, {"payload": cmd.model_dump_json()})
                seeded += 1
                print(
                    f"[seed_monitored_wallets] seeded {wallet} "
                    f"categories={cmd.categories_insider}"
                )
            except Exception as e:
                print(f"[seed_monitored_wallets] XADD failed for {wallet}: {e}", file=sys.stderr)
    finally:
        try:
            r.close()
        except Exception:
            pass

    print(
        f"[seed_monitored_wallets] done: seeded={seeded}, "
        f"already_present={skipped}, total_insiders={len(rows)}"
    )
    return seeded


if __name__ == "__main__":
    try:
        seed()
    except Exception as e:
        print(f"[seed_monitored_wallets] unexpected error: {e}", file=sys.stderr)
    # Always exit 0: best-effort startup step, must never block the orchestrator.
    sys.exit(0)
