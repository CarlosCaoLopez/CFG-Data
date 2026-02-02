# Test Markets Collector
# Usage: python -m scripts.test_markets

import asyncio
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.http_client import get_http_client


async def main():
    """Test fetching closed markets from Gamma API"""

    print("=" * 60)
    print("Testing MarketsCollector - Fetching closed markets")
    print("=" * 60)

    collector = MarketsCollector()

    # Fetch first 5 closed markets
    print("\nFetching 5 closed markets (min $50k volume)...\n")

    client = get_http_client()
    try:
        count = 0
        async for market in collector.fetch_closed_markets(limit=5):
            count += 1
            volume = float(market.get('volume', 0) or 0)
            print(f"--- Market {count} ---")
            print(f"  conditionId: {market.get('conditionId', 'N/A')}")
            print(f"  question: {market.get('question', 'N/A')[:80]}...")
            print(f"  endDate: {market.get('endDate', 'N/A')}")
            print(f"  closed: {market.get('closed', 'N/A')}")
            print(f"  outcomePrices: {market.get('outcomePrices', 'N/A')}")
            print(f"  volume: ${volume:,.2f}")
            print()

        print(f"Total markets fetched: {count}")
    finally:
        await client.close()

    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
