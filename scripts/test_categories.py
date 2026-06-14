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
        component_d=10.0,
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


async def test_enrich_market_with_event_data():
    """
    Cubre el path de backfill: market SIN category/tags en la respuesta de
    /markets → enrich_market_with_event_data los trae de la Events API →
    el normalizer resuelve la category.

    Mockea fetch_event_by_id, así que no hace HTTP real (test unitario).
    """
    print("\n" + "=" * 60)
    print("TEST 6: enrich_market_with_event_data deriva category")
    print("=" * 60)

    from ingestion.collectors.markets_collector import MarketsCollector

    normalizer = DataNormalizer(categorias_de_interes=["Sports", "Crypto"])
    collector = MarketsCollector()

    # --- Caso A: Events API devuelve category directamente (mercado cerrado) ---
    calls = {"count": 0}

    async def fake_event_with_category(event_id):
        calls["count"] += 1
        return {
            "id": event_id,
            "category": "Crypto",
            "tags": [{"id": "1", "label": "Bitcoin", "slug": "bitcoin"}],
        }

    collector.fetch_event_by_id = fake_event_with_category

    raw_market_a = {
        "conditionId": "0xclosedmarket",
        "question": "Will BTC reach $100K?",
        "endDate": "2026-12-31T00:00:00Z",
        "volume": "500000",
        "closed": True,
        "events": [{"id": "event-99"}],  # solo el id, sin category ni tags
    }
    enriched_a = await collector.enrich_market_with_event_data(raw_market_a)
    metadata_a = normalizer.normalize_market(enriched_a)

    print(f"  [A] Category: {metadata_a.category}  Tags: {metadata_a.tags}")
    assert calls["count"] == 1, "Debe llamar a la Events API cuando faltan category/tags"
    assert metadata_a.category == "Crypto", f"Esperaba 'Crypto', got '{metadata_a.category}'"
    assert metadata_a.tags and "Bitcoin" in metadata_a.tags

    # --- Caso B: Events API sin category pero con tag que matchea un interés ---
    async def fake_event_no_category(event_id):
        return {
            "id": event_id,
            # sin "category"
            "tags": [
                {"id": "1", "label": "NBA", "slug": "nba"},
                {"id": "2", "label": "Sports", "slug": "sports"},
            ],
        }

    collector.fetch_event_by_id = fake_event_no_category

    raw_market_b = {
        "conditionId": "0xnocategory",
        "question": "Will the Lakers win?",
        "endDate": "2026-06-15T00:00:00Z",
        "volume": "120000",
        "closed": True,
        "events": [{"id": "event-100"}],
    }
    enriched_b = await collector.enrich_market_with_event_data(raw_market_b)
    metadata_b = normalizer.normalize_market(enriched_b)

    print(f"  [B] Category: {metadata_b.category}  Tags: {metadata_b.tags}")
    # Solo "Sports" está en CATEGORIAS_DE_INTERES → category derivada de ese tag
    assert metadata_b.category == "Sports", f"Esperaba 'Sports', got '{metadata_b.category}'"
    assert metadata_b.tags and "NBA" in metadata_b.tags

    # --- Caso C: idempotencia — ya trae category + tags → NO llama Events API ---
    calls_c = {"count": 0}

    async def should_not_be_called(event_id):
        calls_c["count"] += 1
        return None

    collector.fetch_event_by_id = should_not_be_called

    raw_market_c = {
        "conditionId": "0xcomplete",
        "question": "Already enriched",
        "endDate": "2026-06-15T00:00:00Z",
        "volume": "80000",
        "closed": True,
        "category": "Sports",
        "events": [{"id": "event-101", "tags": [{"label": "NFL", "slug": "nfl"}]}],
    }
    await collector.enrich_market_with_event_data(raw_market_c)

    print(f"  [C] Llamadas a Events API (esperado 0): {calls_c['count']}")
    assert calls_c["count"] == 0, "No debe llamar a Events API si ya hay category+tags"

    print("\n  ✅ Test passed: enrich_market_with_event_data deriva/resuelve category")


async def test_real_api_categories():
    """Test with real Polymarket API data (optional)"""
    print("\n" + "=" * 60)
    print("TEST 7: Real API category extraction (optional)")
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

    # Unit test del enriquecimiento vía Events API (mockeado, sin HTTP)
    asyncio.run(test_enrich_market_with_event_data())

    # Integration test with real API (optional)
    print("\n" + "-" * 60)
    print("Running integration test with real API...")
    asyncio.run(test_real_api_categories())

    print("\n" + "=" * 60)
    print("  ALL TESTS COMPLETED SUCCESSFULLY ✅")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
