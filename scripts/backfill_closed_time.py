# One-off: backfill markets.closed_time for rows stored before the closedTime fix.
#
# Existing closed markets have closed_time = NULL, so the dashboard falls back to
# end_date (the nominal/future date). This fetches the real closedTime from Gamma
# (?closed=true&condition_ids=X) and updates each row.
#
# Usage:  poetry run python -m scripts.backfill_closed_time
#         poetry run python -m scripts.backfill_closed_time --dry-run

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text

from database.connection import async_session
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.normalizers.data_normalizer import DataNormalizer


async def main(dry_run: bool) -> None:
    collector = MarketsCollector()
    normalizer = DataNormalizer()

    async with async_session() as session:
        rows = (await session.execute(text("""
            SELECT market_id, question
            FROM markets
            WHERE closed = true AND closed_time IS NULL
            ORDER BY updated_at DESC
        """))).fetchall()

    print(f"Closed markets without closed_time: {len(rows)}")
    if dry_run:
        print("(dry-run — no writes)")

    updated = 0
    missing = 0
    for i, r in enumerate(rows, 1):
        market_id = r.market_id
        raw = await collector.fetch_closed_market_by_id(market_id)
        closed_time = normalizer._parse_datetime_optional(
            (raw or {}).get("closedTime")
        )

        if closed_time is None:
            missing += 1
            print(f"[{i}/{len(rows)}] {market_id[:20]}… no closedTime in Gamma")
            continue

        if not dry_run:
            async with async_session() as session:
                await session.execute(
                    text("UPDATE markets SET closed_time = :ct WHERE market_id = :mid"),
                    {"ct": closed_time, "mid": market_id},
                )
                await session.commit()
        updated += 1
        print(f"[{i}/{len(rows)}] {market_id[:20]}… → {closed_time}")

    print(f"\nDone. updated={updated} missing_closedTime={missing} total={len(rows)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.dry_run))
