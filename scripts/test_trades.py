# Test Trades Collector
# Usage: python -m scripts.test_trades

import asyncio
from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.collectors.trades_collector import TradesCollector
from ingestion.http_client import get_http_client


async def main():
    """Test fetching trades from Data API"""

    print("=" * 60)
    print("Testing TradesCollector - Fetching trades for a market")
    print("=" * 60)

    markets_collector = MarketsCollector()
    trades_collector = TradesCollector()

    client = get_http_client()
    try:
        # First, get one closed market
        print("\nStep 1: Getting a closed market...\n")

        market = None
        async for m in markets_collector.fetch_closed_markets(limit=1):
            market = m
            break

        if not market:
            print("No closed markets found!")
            return

        market_id = market.get("conditionId")
        print(f"  Market: {market.get('question', 'N/A')[:60]}...")
        print(f"  conditionId: {market_id}")
        print()

        # Now fetch trades for this market
        print("Step 2: Fetching first 10 trades for this market...\n")

        trades = []
        async for trade in trades_collector.fetch_trades_for_market(market_id, limit=10):
            trades.append(trade)

        if not trades:
            print("No trades found for this market!")
            return

        # Show first trade raw to see field names
        print("First trade raw fields:")
        first_trade = trades[0]
        for key in list(first_trade.keys())[:15]:
            print(f"  {key}: {str(first_trade.get(key))[:50]}")
        print()

        # Verify trades belong to market
        print(f"Expected market conditionId: {market_id[:30]}...\n")

        for i, trade in enumerate(trades, 1):
            price = float(trade.get("price") or 0)
            size = float(trade.get("size") or 0)
            # usdcSize = size * price (size is in tokens)
            usdc_size = size * price
            tx_hash = trade.get("transactionHash") or trade.get("txHash") or trade.get("hash") or "N/A"
            wallet = trade.get("proxyWallet") or trade.get("user") or trade.get("maker") or "N/A"
            trade_condition = trade.get("conditionId", "N/A")

            # Check if trade belongs to our market
            matches_market = market_id.lower().startswith(trade_condition.lower()[:40]) if trade_condition != "N/A" else False

            print(f"--- Trade {i} ---")
            print(f"  conditionId: {trade_condition[:30]}... {'✓' if matches_market else '✗ MISMATCH'}")
            print(f"  txHash: {str(tx_hash)[:20]}...")
            print(f"  wallet: {str(wallet)[:20]}...")
            print(f"  side: {trade.get('side', 'N/A')}")
            print(f"  outcome: {trade.get('outcome', 'N/A')}")
            print(f"  price: {price:.4f}")
            print(f"  size: {size:.2f} tokens")
            print(f"  usdcSize: ${usdc_size:,.2f} (size * price)")
            print(f"  timestamp: {trade.get('timestamp', 'N/A')}")
            print()

        # Extract unique wallets
        wallets = trades_collector.extract_wallets_from_trades(trades)
        print(f"Unique wallets in these trades: {len(wallets)}")

    finally:
        await client.close()

    print("\n" + "=" * 60)
    print("Test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
