# Test Publisher (Redis via queue_system)
# Usage: python -m scripts.test_publisher
# Requires: docker-compose up -d redis

import asyncio

from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.wallet_age_collector import WalletAgeCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.publisher import get_publisher
from ingestion.http_client import get_http_client


async def main():
    """Test publishing MarketDataBatch to Redis via queue_system"""

    print("=" * 60)
    print("Testing RedisPublisher → queue_system Integration")
    print("=" * 60)

    publisher = get_publisher()

    # Step 1: Check Redis health
    print("\n[1] Checking Redis connection...")

    healthy = await publisher.health_check()
    if not healthy:
        print("    ✗ Redis is NOT available!")
        print("    Run: docker-compose up -d redis")
        await publisher.close()
        return

    print("    ✓ Redis connection OK")

    # Step 2: Get stream info
    print("\n[2] Getting stream info (before)...")

    info = await publisher.get_stream_info()
    if "error" in info:
        print(f"    ✗ Error: {info['error']}")
    else:
        stream_info = info.get("raw_data_stream", {})
        print(f"    Stream: {stream_info.get('name', 'unknown')}")
        print(f"    Length: {stream_info.get('length', 0)} messages")

    # Step 3: Fetch data and create batch
    print("\n[3] Fetching data to create a batch...")

    markets_collector = MarketsCollector()
    trades_collector = TradesCollector()
    wallet_age_collector = WalletAgeCollector()
    normalizer = DataNormalizer()
    http_client = get_http_client()

    try:
        # Get a market
        market_raw = None
        async for m in markets_collector.fetch_closed_markets(limit=1):
            market_raw = m
            break

        if not market_raw:
            print("    ✗ No closed markets found!")
            await publisher.close()
            return

        market_id = market_raw.get("conditionId")
        print(f"    Market: {market_raw.get('question', 'N/A')[:50]}...")

        # Get trades
        trades_raw = []
        async for trade in trades_collector.fetch_trades_for_market(market_id, limit=10):
            trades_raw.append(trade)

        print(f"    Trades: {len(trades_raw)}")

        # Get wallet ages (minimal, no Polygonscan)
        wallets = trades_collector.extract_wallets_from_trades(trades_raw)
        wallets_subset = set(list(wallets)[:3])
        wallet_ages_raw = await wallet_age_collector.fetch_wallet_ages_batch(
            wallets_subset,
            use_polygonscan=False
        )

        print(f"    Wallets: {len(wallet_ages_raw)}")

        # Normalize
        batch = normalizer.normalize_batch(
            market_raw=market_raw,
            trades_raw=trades_raw,
            wallet_ages_raw=wallet_ages_raw,
        )

        print(f"\n    MarketDataBatch ready:")
        print(f"      trades: {len(batch.trades)}")
        print(f"      wallets: {len(batch.wallet_ages)}")

    finally:
        await http_client.close()

    # Step 4: Publish batch
    print("\n[4] Publishing batch to Redis (via queue_system)...")

    batch_id = await publisher.publish_market_batch(batch)
    print(f"    ✓ Published with message_id: {batch_id}")

    # Step 5: Verify publication
    print("\n[5] Getting stream info (after)...")

    info = await publisher.get_stream_info()
    if "error" not in info:
        stream_info = info.get("raw_data_stream", {})
        print(f"    Stream: {stream_info.get('name', 'unknown')}")
        print(f"    Length: {stream_info.get('length', 0)} messages")

    # Cleanup
    await publisher.close()

    print("\n" + "=" * 60)
    print("✓ Publisher test successful!")
    print("=" * 60)
    print("\nThe message is now in the queue_system stream.")
    print("Persona B (Orchestrator) can consume it from there.")


if __name__ == "__main__":
    asyncio.run(main())
