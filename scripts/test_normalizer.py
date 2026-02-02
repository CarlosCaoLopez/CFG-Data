# Test Data Normalizer
# Usage: python -m scripts.test_normalizer

import asyncio

from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.wallet_age_collector import WalletAgeCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.http_client import get_http_client


async def main():
    """Test the data normalizer with real API data"""

    print("=" * 60)
    print("Testing DataNormalizer - Converting raw data to contracts")
    print("=" * 60)

    markets_collector = MarketsCollector()
    trades_collector = TradesCollector()
    wallet_age_collector = WalletAgeCollector()
    normalizer = DataNormalizer()

    client = get_http_client()
    try:
        # Step 1: Get a closed market
        print("\nStep 1: Fetching a closed market...\n")

        market_raw = None
        async for m in markets_collector.fetch_closed_markets(limit=1):
            market_raw = m
            break

        if not market_raw:
            print("No closed markets found!")
            return

        market_id = market_raw.get("conditionId")
        print(f"  Market: {market_raw.get('question', 'N/A')[:50]}...")
        print(f"  conditionId: {market_id[:30]}...")

        # Step 2: Normalize market
        print("\nStep 2: Normalizing market metadata...\n")

        market_normalized = normalizer.normalize_market(market_raw)
        print(f"  RawMarketMetadata:")
        print(f"    market_id: {market_normalized.market_id[:30]}...")
        print(f"    question: {market_normalized.question[:50]}...")
        print(f"    end_date: {market_normalized.end_date}")
        print(f"    closed: {market_normalized.closed}")
        print(f"    outcome_prices: {market_normalized.outcome_prices}")
        print(f"    volume_usdc: ${market_normalized.volume_usdc:,.2f}")

        # Step 3: Fetch and normalize trades
        print("\nStep 3: Fetching and normalizing trades...\n")

        trades_raw = []
        async for trade in trades_collector.fetch_trades_for_market(market_id, limit=5):
            trades_raw.append(trade)

        if not trades_raw:
            print("  No trades found for this market!")
        else:
            print(f"  Fetched {len(trades_raw)} trades")

            for i, trade_raw in enumerate(trades_raw[:3], 1):
                trade_normalized = normalizer.normalize_trade(trade_raw, market_id)
                print(f"\n  RawTrade {i}:")
                print(f"    tx_hash: {trade_normalized.tx_hash[:20]}...")
                print(f"    wallet: {trade_normalized.wallet[:20]}...")
                print(f"    market_id: {trade_normalized.market_id[:20]}...")
                print(f"    timestamp: {trade_normalized.timestamp}")
                print(f"    side: {trade_normalized.side}")
                print(f"    outcome: {trade_normalized.outcome}")
                print(f"    price: {trade_normalized.price:.4f}")
                print(f"    size_usdc: ${trade_normalized.size_usdc:,.2f}")
                print(f"    size_tokens: {trade_normalized.size_tokens:.2f}")

        # Step 4: Extract wallets and fetch ages
        print("\n\nStep 4: Fetching wallet ages...\n")

        wallets = trades_collector.extract_wallets_from_trades(trades_raw)
        print(f"  Found {len(wallets)} unique wallets")

        # Only fetch first 2 wallets to save time
        wallets_subset = set(list(wallets)[:2])
        wallet_ages_raw = await wallet_age_collector.fetch_wallet_ages_batch(
            wallets_subset,
            use_polygonscan=False  # Skip Polygonscan for speed
        )

        for wallet, age_data in wallet_ages_raw.items():
            age_normalized = normalizer.normalize_wallet_age(age_data)
            print(f"\n  RawWalletAge:")
            print(f"    wallet: {age_normalized.wallet[:20]}...")
            print(f"    first_tx_polymarket: {age_normalized.first_tx_polymarket}")
            print(f"    first_tx_polygon: {age_normalized.first_tx_polygon}")

        # Step 5: Create full batch
        print("\n\nStep 5: Creating MarketDataBatch...\n")

        batch = normalizer.normalize_batch(
            market_raw=market_raw,
            trades_raw=trades_raw,
            wallet_ages_raw=wallet_ages_raw,
        )

        print(f"  MarketDataBatch created:")
        print(f"    metadata.market_id: {batch.metadata.market_id[:30]}...")
        print(f"    trades count: {len(batch.trades)}")
        print(f"    wallet_ages count: {len(batch.wallet_ages)}")
        print(f"    ingested_at: {batch.ingested_at}")

        # Step 6: Verify serialization
        print("\n\nStep 6: Testing JSON serialization...\n")

        json_str = batch.model_dump_json()
        print(f"  JSON size: {len(json_str):,} bytes")
        print(f"  First 200 chars: {json_str[:200]}...")

        print("\n  All normalizations successful!")

    finally:
        await client.close()

    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
