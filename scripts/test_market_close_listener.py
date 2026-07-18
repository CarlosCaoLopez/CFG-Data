# Test script for L1 MarketCloseListener
# Validates: hybrid discovery, category filtering, close detection logic
#
# Usage:
#   poetry run python -m scripts.test_market_close_listener

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.config import get_settings
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.listeners.market_close_listener import MarketCloseListener
from contracts.raw_market_data import RawMarketMetadata


def test_category_filtering_raw():
    """Test _matches_categories with raw market dicts."""
    print("\n" + "=" * 60)
    print("TEST 1: Category filtering (raw market dict)")
    print("=" * 60)

    listener = MarketCloseListener()

    # Market with matching category
    market_sports = {
        "conditionId": "0x111",
        "category": "Sports",
        "events": [],
    }
    assert listener._matches_categories(market_sports), "Sports should match"
    print("  Sports category → matched ✅")

    # Market with matching tag (case insensitive)
    market_crypto_tag = {
        "conditionId": "0x222",
        "events": [
            {"tags": [{"label": "Crypto", "slug": "crypto"}]}
        ],
    }
    assert listener._matches_categories(market_crypto_tag), "Crypto tag should match"
    print("  Crypto tag → matched ✅")

    # Market with string tags
    market_string_tags = {
        "conditionId": "0x333",
        "events": [
            {"tags": ["Sports", "NFL"]}
        ],
    }
    assert listener._matches_categories(market_string_tags), "String tag 'Sports' should match"
    print("  String tag 'Sports' → matched ✅")

    # Market with no matching category/tags
    market_no_match = {
        "conditionId": "0x444",
        "category": "Science",
        "events": [
            {"tags": [{"label": "Physics", "slug": "physics"}]}
        ],
    }
    assert not listener._matches_categories(market_no_match), "Science should NOT match"
    print("  Science category → NOT matched ✅")

    # Market with no category at all
    market_empty = {
        "conditionId": "0x555",
    }
    assert not listener._matches_categories(market_empty), "Empty should NOT match"
    print("  Empty market → NOT matched ✅")

    print("\n  ✅ All category filtering tests passed")


def test_category_filtering_metadata():
    """Test _metadata_matches_categories with RawMarketMetadata."""
    print("\n" + "=" * 60)
    print("TEST 2: Category filtering (normalized metadata)")
    print("=" * 60)

    listener = MarketCloseListener()

    # Metadata with matching category
    meta_sports = RawMarketMetadata(
        market_id="0x111",
        question="Will Lakers win?",
        end_date=datetime.now(timezone.utc),
        closed=False,
        volume_usdc=50000.0,
        category="Sports",
        tags=["NBA", "Basketball"],
    )
    assert listener._metadata_matches_categories(meta_sports)
    print("  Sports category → matched ✅")

    # Metadata with matching tag only
    meta_tag = RawMarketMetadata(
        market_id="0x222",
        question="BTC price?",
        end_date=datetime.now(timezone.utc),
        closed=False,
        volume_usdc=100000.0,
        category=None,
        tags=["Crypto", "Bitcoin"],
    )
    assert listener._metadata_matches_categories(meta_tag)
    print("  Crypto tag (no category) → matched ✅")

    # Metadata with no match
    meta_no = RawMarketMetadata(
        market_id="0x333",
        question="Mars landing?",
        end_date=datetime.now(timezone.utc),
        closed=False,
        volume_usdc=10000.0,
        category="Space",
        tags=["NASA"],
    )
    assert not listener._metadata_matches_categories(meta_no)
    print("  Space category → NOT matched ✅")

    print("\n  ✅ All metadata filtering tests passed")


def test_winning_outcome():
    """Test compute_winning_outcome on DataNormalizer."""
    print("\n" + "=" * 60)
    print("TEST 3: Winning outcome computation")
    print("=" * 60)

    normalizer = DataNormalizer()

    assert normalizer.compute_winning_outcome([1.0, 0.0]) == "Yes"
    print("  [1.0, 0.0] → Yes ✅")

    assert normalizer.compute_winning_outcome([0.0, 1.0]) == "No"
    print("  [0.0, 1.0] → No ✅")

    assert normalizer.compute_winning_outcome([0.5, 0.5]) is None
    print("  [0.5, 0.5] → None (unresolved) ✅")

    assert normalizer.compute_winning_outcome(None) is None
    print("  None → None ✅")

    assert normalizer.compute_winning_outcome([0.99, 0.01]) == "Yes"
    print("  [0.99, 0.01] → Yes ✅")

    print("\n  ✅ All winning outcome tests passed")


def test_mark_last_trade():
    """Test mark_last_trade on DataNormalizer."""
    print("\n" + "=" * 60)
    print("TEST 4: Mark last trade")
    print("=" * 60)

    from contracts.raw_market_data import RawTrade

    normalizer = DataNormalizer()

    trades = [
        RawTrade(
            tx_hash="0xaaa", wallet="0x1", market_id="0xm",
            timestamp=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            side="BUY", outcome="Yes", price=0.5, size_usdc=100, size_tokens=200,
        ),
        RawTrade(
            tx_hash="0xbbb", wallet="0x2", market_id="0xm",
            timestamp=datetime(2026, 1, 1, 15, 0, tzinfo=timezone.utc),
            side="SELL", outcome="No", price=0.3, size_usdc=50, size_tokens=166,
        ),
        RawTrade(
            tx_hash="0xccc", wallet="0x3", market_id="0xm",
            timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            side="BUY", outcome="Yes", price=0.6, size_usdc=200, size_tokens=333,
        ),
    ]

    normalizer.mark_last_trade(trades)

    assert trades[0].last_trade is False
    assert trades[1].last_trade is True  # 15:00 is latest
    assert trades[2].last_trade is False

    print("  Trade at 10:00 → last_trade=False ✅")
    print("  Trade at 15:00 → last_trade=True ✅")
    print("  Trade at 12:00 → last_trade=False ✅")

    print("\n  ✅ All mark_last_trade tests passed")


async def test_track_market_from_metadata():
    """Test that _track_market_from_metadata adds to tracked dict."""
    print("\n" + "=" * 60)
    print("TEST 5: Track market from metadata")
    print("=" * 60)

    listener = MarketCloseListener()

    meta = RawMarketMetadata(
        market_id="0xtest123",
        question="Test market?",
        end_date=datetime(2026, 6, 1, tzinfo=timezone.utc),
        closed=False,
        volume_usdc=75000.0,
        category="Crypto",
        tags=["Bitcoin", "BTC"],
    )

    assert listener.tracked_count == 0
    await listener._track_market_from_metadata(meta)
    assert listener.tracked_count == 1
    assert "0xtest123" in listener._tracked_markets

    # Minimal persisted shape: just question/category/tags (see
    # tracked_markets_store.py) — no conditionId (redundant with the dict key)
    # or the full raw Gamma blob.
    tracked = listener._tracked_markets["0xtest123"]
    assert tracked["question"] == "Test market?"
    assert tracked["category"] == "Crypto"
    assert tracked["tags"] == ["Bitcoin", "BTC"]

    print(f"  Tracked market: 0xtest123...")
    print(f"  Category: {tracked['category']}")
    print(f"  Tags: {tracked['tags']}")

    print("\n  ✅ Track market test passed")


def test_config_categories():
    """Test that CATEGORIAS_DE_INTERES loads correctly."""
    print("\n" + "=" * 60)
    print("TEST 6: Config CATEGORIAS_DE_INTERES")
    print("=" * 60)

    settings = get_settings()

    print(f"  Categories: {settings.categorias_de_interes}")
    print(f"  L1 poll interval: {settings.l1_market_poll_interval_seconds}s")
    print(f"  L1 gamma discovery interval: {settings.l1_gamma_discovery_interval_seconds}s")
    print(f"  L1 min volume: ${settings.l1_min_volume_usdc:,.0f}")
    print(f"  Closed markets stream: {settings.closed_markets_stream}")

    assert len(settings.categorias_de_interes) > 0, "Should have at least one category"
    assert settings.l1_market_poll_interval_seconds > 0
    assert settings.closed_markets_stream == "closed_markets"

    print("\n  ✅ Config test passed")


async def test_gamma_discovery_live():
    """Integration test: fetch open markets and filter by categories (live API)."""
    print("\n" + "=" * 60)
    print("TEST 7: Live Gamma API discovery (integration)")
    print("=" * 60)

    try:
        from ingestion.collectors.markets_collector import MarketsCollector

        collector = MarketsCollector()
        normalizer = DataNormalizer()
        listener = MarketCloseListener()
        settings = get_settings()

        print(f"  Fetching open markets (min volume: ${settings.l1_min_volume_usdc:,.0f})...")

        count = 0
        matched = 0

        async for market_raw in collector.fetch_open_markets(
            min_volume_usdc=settings.l1_min_volume_usdc, limit=10
        ):
            count += 1
            condition_id = market_raw.get("conditionId", "")

            # Enrich via Events API
            events = market_raw.get("events", [])
            event_id = None
            if events and isinstance(events, list) and len(events) > 0:
                event_id = events[0].get("id")

            if event_id:
                event_data = await collector.fetch_event_by_id(str(event_id))
                if event_data:
                    event_category = event_data.get("category")
                    if event_category:
                        market_raw["category"] = event_category
                    event_tags = event_data.get("tags", [])
                    if event_tags and market_raw.get("events"):
                        market_raw["events"][0]["tags"] = event_tags

            category = market_raw.get("category", "N/A")
            matches = listener._matches_categories(market_raw)

            if matches:
                matched += 1

            tag_labels = []
            for ev in market_raw.get("events", []):
                for t in ev.get("tags", []):
                    if isinstance(t, dict):
                        tag_labels.append(t.get("label", ""))
                    elif isinstance(t, str):
                        tag_labels.append(t)

            status = "MATCH" if matches else "skip"
            print(f"\n  [{status}] {market_raw.get('question', '')[:50]}...")
            print(f"    Category: {category} | Tags: {tag_labels[:5]}")

        print(f"\n  Total fetched: {count}")
        print(f"  Matching categories: {matched}")

        print("\n  ✅ Live discovery test completed")

    except Exception as e:
        print(f"  ⚠️ Skipped: {e}")


def main():
    print("\n" + "=" * 60)
    print("  L1 MARKET CLOSE LISTENER - TEST SUITE")
    print("=" * 60)

    # Unit tests
    test_category_filtering_raw()
    test_category_filtering_metadata()
    test_winning_outcome()
    test_mark_last_trade()
    asyncio.run(test_track_market_from_metadata())
    test_config_categories()

    # Integration test (live API)
    print("\n" + "-" * 60)
    print("Running integration test with live API...")
    asyncio.run(test_gamma_discovery_live())

    print("\n" + "=" * 60)
    print("  ALL L1 TESTS COMPLETED SUCCESSFULLY ✅")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
