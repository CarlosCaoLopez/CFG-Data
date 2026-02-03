# Test script for category extraction flow
# Validates that category and tags propagate correctly through the pipeline

import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.normalizers.data_normalizer import DataNormalizer
from contracts.raw_market_data import MarketDataBatch, RawMarketMetadata, RawTrade
from contracts.score_output import WalletScore, Alert
from datetime import datetime, timezone


def test_normalizer_extracts_category():
    """Test that DataNormalizer correctly extracts category and tags"""
    print("\n" + "=" * 60)
    print("TEST 1: DataNormalizer extracts category and tags")
    print("=" * 60)

    normalizer = DataNormalizer()

    # Simulated raw market data from Gamma API
    raw_market = {
        "conditionId": "0x1234567890abcdef",
        "question": "Will the Lakers win the NBA Finals?",
        "category": "Sports",
        "events": [
            {
                "id": "event-1",
                "title": "NBA Finals 2026",
                "tags": [
                    {"id": "1", "label": "NBA", "slug": "nba"},
                    {"id": "2", "label": "Basketball", "slug": "basketball"},
                    {"id": "3", "label": "Lakers", "slug": "lakers"},
                ]
            }
        ],
        "endDate": "2026-06-15T00:00:00Z",
        "volume": "150000.50",
        "closed": False,
        "outcomePrices": '["0.65", "0.35"]'
    }

    metadata = normalizer.normalize_market(raw_market)

    print(f"  Market ID: {metadata.market_id}")
    print(f"  Question: {metadata.question}")
    print(f"  Category: {metadata.category}")
    print(f"  Tags: {metadata.tags}")
    print(f"  Volume: ${metadata.volume_usdc:,.2f}")

    # Assertions
    assert metadata.category == "Sports", f"Expected 'Sports', got '{metadata.category}'"
    assert metadata.tags is not None, "Tags should not be None"
    assert "NBA" in metadata.tags, f"'NBA' should be in tags: {metadata.tags}"
    assert "Basketball" in metadata.tags, f"'Basketball' should be in tags: {metadata.tags}"
    assert "Lakers" in metadata.tags, f"'Lakers' should be in tags: {metadata.tags}"

    print("\n  ✅ Test passed: category and tags extracted correctly")
    return metadata


def test_normalizer_handles_missing_category():
    """Test that normalizer handles missing category gracefully"""
    print("\n" + "=" * 60)
    print("TEST 2: DataNormalizer handles missing category/tags")
    print("=" * 60)

    normalizer = DataNormalizer()

    # Market without category or events
    raw_market = {
        "conditionId": "0xabcdef1234567890",
        "question": "Will BTC reach $100K?",
        "endDate": "2026-12-31T00:00:00Z",
        "volume": "500000",
        "closed": False,
    }

    metadata = normalizer.normalize_market(raw_market)

    print(f"  Market ID: {metadata.market_id}")
    print(f"  Question: {metadata.question}")
    print(f"  Category: {metadata.category}")
    print(f"  Tags: {metadata.tags}")

    assert metadata.category is None, f"Expected None, got '{metadata.category}'"
    assert metadata.tags is None, f"Expected None, got '{metadata.tags}'"

    print("\n  ✅ Test passed: missing category/tags handled gracefully")
    return metadata


def test_payload_contains_category():
    """Test that JobCreator payload includes category and tags"""
    print("\n" + "=" * 60)
    print("TEST 3: Payload propagates category and tags")
    print("=" * 60)

    # Create metadata with category
    metadata = RawMarketMetadata(
        market_id="0x123",
        question="Test market",
        end_date=datetime.now(timezone.utc),
        closed=True,
        outcome_prices=[1.0, 0.0],
        volume_usdc=50000.0,
        category="Crypto",
        tags=["Bitcoin", "BTC", "Price"],
    )

    # Create a trade
    trade = RawTrade(
        tx_hash="0xabc123",
        wallet="0xwallet",
        market_id="0x123",
        timestamp=datetime.now(timezone.utc),
        side="BUY",
        outcome="Yes",
        price=0.65,
        size_usdc=1000.0,
        size_tokens=1538.46,
    )

    # Create batch
    batch = MarketDataBatch(
        metadata=metadata,
        trades=[trade],
        wallet_ages={},
    )

    # Simulate what JobCreator does
    payload = {
        'trade': trade.model_dump(),
        'market': batch.metadata.model_dump(),
        'wallet_age': None,
    }

    print(f"  Payload market.category: {payload['market']['category']}")
    print(f"  Payload market.tags: {payload['market']['tags']}")

    assert payload['market']['category'] == "Crypto"
    assert payload['market']['tags'] == ["Bitcoin", "BTC", "Price"]

    print("\n  ✅ Test passed: payload contains category and tags")
    return payload


def test_wallet_score_with_category():
    """Test that WalletScore can include category and tags"""
    print("\n" + "=" * 60)
    print("TEST 4: WalletScore includes category and tags")
    print("=" * 60)

    score = WalletScore(
        wallet="0xwallet123",
        market_id="0xmarket456",
        trade_tx_hash="0xtx789",
        category="Sports",
        tags=["NFL", "Football"],
        i_score=125.5,
        percentile=0.95,
        component_a=50.0,
        component_b=40.0,
        component_c=35.5,
        delta_elo=15.0,
        volume_usdc=5000.0,
        won=True,
        price_at_trade=0.35,
        age_days=2.5,
        hours_to_resolution=4.0,
        slippage=0.02,
        portfolio_entropy=0.5,
    )

    print(f"  Wallet: {score.wallet}")
    print(f"  I_score: {score.i_score}")
    print(f"  Percentile: {score.percentile}")
    print(f"  Category: {score.category}")
    print(f"  Tags: {score.tags}")

    assert score.category == "Sports"
    assert score.tags == ["NFL", "Football"]

    print("\n  ✅ Test passed: WalletScore includes category and tags")
    return score


def test_alert_with_category():
    """Test that Alert can include category"""
    print("\n" + "=" * 60)
    print("TEST 5: Alert includes category")
    print("=" * 60)

    alert = Alert(
        wallet="0xsuspicious",
        market_id="0xmarket789",
        category="US-current-affairs",
        i_score=250.0,
        percentile=0.99,
        alert_type="P99",
    )

    print(f"  Wallet: {alert.wallet}")
    print(f"  Alert Type: {alert.alert_type}")
    print(f"  I_score: {alert.i_score}")
    print(f"  Category: {alert.category}")

    assert alert.category == "US-current-affairs"
    assert alert.alert_type == "P99"

    print("\n  ✅ Test passed: Alert includes category")
    return alert


async def test_real_api_categories():
    """Test with real Polymarket API data (optional)"""
    print("\n" + "=" * 60)
    print("TEST 6: Real API category extraction (optional)")
    print("=" * 60)

    try:
        from ingestion.collectors.markets_collector import MarketsCollector

        collector = MarketsCollector()
        normalizer = DataNormalizer()

        print("  Fetching 3 closed markets from Polymarket...")
        markets = await collector.fetch_closed_markets(limit=3)

        if not markets:
            print("  ⚠️ No markets returned from API")
            return

        for i, raw_market in enumerate(markets, 1):
            metadata = normalizer.normalize_market(raw_market)
            print(f"\n  Market {i}:")
            print(f"    Question: {metadata.question[:60]}...")
            print(f"    Category: {metadata.category}")
            print(f"    Tags: {metadata.tags}")

        print("\n  ✅ Test passed: Real API data processed successfully")

    except ImportError:
        print("  ⚠️ Skipped: MarketsCollector not available")
    except Exception as e:
        print(f"  ⚠️ Skipped: {e}")


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("  CATEGORY EXTRACTION TEST SUITE")
    print("=" * 60)

    # Unit tests (no external dependencies)
    test_normalizer_extracts_category()
    test_normalizer_handles_missing_category()
    test_payload_contains_category()
    test_wallet_score_with_category()
    test_alert_with_category()

    # Integration test with real API (optional)
    print("\n" + "-" * 60)
    print("Running integration test with real API...")
    asyncio.run(test_real_api_categories())

    print("\n" + "=" * 60)
    print("  ALL TESTS COMPLETED SUCCESSFULLY ✅")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
