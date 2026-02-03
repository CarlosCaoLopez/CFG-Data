# Test Wallet Age Collector
# Usage: python -m scripts.test_wallet_age

import asyncio
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.collectors.wallet_age_collector import WalletAgeCollector
from ingestion.http_client import get_http_client
from ingestion.config import get_settings


async def main():
    """Test fetching wallet ages from Data API and Polygonscan"""

    print("=" * 60)
    print("Testing WalletAgeCollector")
    print("=" * 60)

    settings = get_settings()
    markets_collector = MarketsCollector()
    trades_collector = TradesCollector()
    age_collector = WalletAgeCollector()

    # Check if Etherscan API key is configured
    has_etherscan = bool(settings.etherscan_api_key and
                         settings.etherscan_api_key != "your_key_here")
    print(f"\nEtherscan API key configured: {has_etherscan}")

    client = get_http_client()
    try:
        # Step 1: Get a closed market
        print("\nStep 1: Getting a closed market...\n")

        market = None
        async for m in markets_collector.fetch_closed_markets(limit=1):
            market = m
            break

        if not market:
            print("No closed markets found!")
            return

        market_id = market.get("conditionId")
        print(f"  Market: {market.get('question', 'N/A')[:50]}...")

        # Step 2: Get some trades to extract wallets
        print("\nStep 2: Getting trades to find wallets...\n")

        trades = []
        async for trade in trades_collector.fetch_trades_for_market(market_id, limit=5):
            trades.append(trade)

        if not trades:
            print("No trades found!")
            return

        wallets = trades_collector.extract_wallets_from_trades(trades)
        print(f"  Found {len(wallets)} unique wallets")

        # Step 3: Fetch wallet ages
        print("\nStep 3: Fetching wallet ages...\n")

        wallet_ages = await age_collector.fetch_wallet_ages_batch(
            wallets,
            use_polygonscan=has_etherscan
        )

        for wallet, age_info in wallet_ages.items():
            print(f"--- Wallet: {wallet[:15]}... ---")

            polymarket_date = age_info.get("first_tx_polymarket")
            if polymarket_date:
                print(f"  First Polymarket tx: {polymarket_date.isoformat()}")
            else:
                print(f"  First Polymarket tx: N/A")

            polygon_date = age_info.get("first_tx_polygon")
            if polygon_date:
                print(f"  First Polygon tx: {polygon_date.isoformat()}")
            else:
                print(f"  First Polygon tx: N/A (need API key)")
            print()

    finally:
        await client.close()

    print("=" * 60)
    print("Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
