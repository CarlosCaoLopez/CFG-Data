# Test Backfill Runner (Dry Run)
# Usage: python -m scripts.test_backfill
# Does NOT require Redis (dry run mode)

import asyncio

from ingestion.backfill_runner import BackfillRunner


async def main():
    """Test backfill runner in dry-run mode (no Redis needed)"""

    print("=" * 60)
    print("Testing BackfillRunner - Dry Run Mode")
    print("=" * 60)
    print("\nThis test processes real data but does NOT publish to Redis.")
    print("Use this to verify the full pipeline works end-to-end.\n")

    # Create runner in dry-run mode
    runner = BackfillRunner(
        days_back=30,           # Last 30 days
        min_volume=50000,       # $50k minimum volume
        use_polygonscan=False,  # Skip Polygonscan (no API key needed)
        dry_run=True,           # Don't publish to Redis
        max_wallets_per_market=5,   # Limit wallets for testing speed
        max_trades_per_market=10,   # Limit trades for testing speed
    )

    # Clear any existing checkpoint to start fresh
    runner.clear_checkpoint()

    # Run backfill for 2 markets (limited trades per market for speed)
    print("Processing 2 markets in dry-run mode (10 trades each)...\n")

    stats = await runner.run(limit=2)

    # Print summary
    print("\n" + "=" * 60)
    print("Backfill Dry Run Summary")
    print("=" * 60)
    print(f"  Markets processed: {stats['markets_processed']}")
    print(f"  Markets skipped:   {stats['markets_skipped']}")
    print(f"  Total trades:      {stats['total_trades']}")
    print(f"  Total wallets:     {stats['total_wallets']}")
    print(f"  Errors:            {stats['errors']}")
    print(f"  Started:           {stats['started_at']}")
    print(f"  Finished:          {stats['finished_at']}")
    print("=" * 60)

    if stats['errors'] == 0 and stats['markets_processed'] > 0:
        print("\n  Backfill pipeline test successful!")
    else:
        print("\n  Some issues occurred during the test.")

    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
