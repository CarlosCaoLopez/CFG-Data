# Debug script: trace category/tags flow step by step
# Usage: poetry run python -m scripts.debug_categories
#
# Calls real Polymarket APIs to trace where categories get lost.
# Instant — no waiting, no Redis, no long polling.

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, UTC

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.collectors.markets_collector import MarketsCollector
from ingestion.normalizers.data_normalizer import DataNormalizer
from ingestion.http_client import get_http_client


def header(step, title):
    print(f"\n{'=' * 70}")
    print(f"  PASO {step}: {title}")
    print(f"{'=' * 70}")


def show(label, value, expect_truthy=True):
    status = "✅" if (bool(value) == expect_truthy) else "❌ FAIL"
    print(f"  {status}  {label}: {value}")


async def main():
    collector = MarketsCollector()
    normalizer = DataNormalizer()
    client = get_http_client()

    try:
        # ============================================================
        # PASO 1: GET /markets?closed=true — referencia (debería tener todo)
        # ============================================================
        header(1, "GET /markets?closed=true (referencia)")

        closed_market = None
        async for m in collector.fetch_closed_markets(limit=1):
            closed_market = m
            break

        if not closed_market:
            print("  ❌ No se pudo obtener ningun market cerrado")
            return

        condition_id = closed_market["conditionId"]
        print(f"  Market: {closed_market.get('question', '')[:60]}...")
        print(f"  conditionId: {condition_id}")
        show("category (singular)", closed_market.get("category"))
        show("categories (plural)", closed_market.get("categories"))
        show("tags (top-level)", closed_market.get("tags"))
        show("events existe", closed_market.get("events"))

        # Show categories detail if present
        top_cats = closed_market.get("categories", [])
        if top_cats:
            cat_labels = [c.get("label") for c in top_cats if isinstance(c, dict)]
            print(f"         categories labels: {cat_labels}")
        top_tags = closed_market.get("tags", [])
        if top_tags:
            tag_labels = [t.get("label") for t in top_tags if isinstance(t, dict)]
            print(f"         tags labels: {tag_labels}")

        events = closed_market.get("events", [])
        if events and isinstance(events, list) and len(events) > 0:
            event = events[0]
            show("events[0].id", event.get("id"))
            raw_tags = event.get("tags", [])
            show("events[0].tags", raw_tags)
            if raw_tags:
                print(f"         Primer tag (tipo {type(raw_tags[0]).__name__}): {raw_tags[0]}")

        # ============================================================
        # PASO 2: GET /markets?condition_ids=X — lo que usa _check_for_closures
        # ============================================================
        header(2, f"GET /markets?condition_ids={condition_id[:20]}...")

        market_by_id = await collector.fetch_market_by_id(condition_id)
        if not market_by_id:
            print("  ❌ fetch_market_by_id retorno None")
            return

        show("category (singular)", market_by_id.get("category"), expect_truthy=False)
        show("categories (plural)", market_by_id.get("categories"))
        show("tags (top-level)", market_by_id.get("tags"))
        print(f"         ^ category singular esperado None, pero categories plural podria tener datos")

        # Show categories detail if present
        top_cats2 = market_by_id.get("categories", [])
        if top_cats2:
            cat_labels2 = [c.get("label") for c in top_cats2 if isinstance(c, dict)]
            print(f"         categories labels: {cat_labels2}")
        top_tags2 = market_by_id.get("tags", [])
        if top_tags2:
            tag_labels2 = [t.get("label") for t in top_tags2 if isinstance(t, dict)]
            print(f"         tags labels: {tag_labels2}")

        events2 = market_by_id.get("events", [])
        show("events existe", events2)

        event_id = None
        if events2 and isinstance(events2, list) and len(events2) > 0:
            event_id = events2[0].get("id")
            show("events[0].id", event_id)
            raw_tags2 = events2[0].get("tags", [])
            show("events[0].tags", raw_tags2, expect_truthy=False)
            print(f"         ^ Esperado: vacio (este endpoint no devuelve tags)")
        else:
            print("  ❌ events esta vacio — no se puede extraer event_id!")
            return

        # ============================================================
        # PASO 3: GET /events/{event_id} — re-fetch (el fix)
        # ============================================================
        header(3, f"GET /events/{event_id}")

        if not event_id:
            print("  ❌ No hay event_id para hacer re-fetch")
            return

        event_data = await collector.fetch_event_by_id(str(event_id))
        if not event_data:
            print("  ❌ fetch_event_by_id retorno None")
            return

        print(f"  Response keys: {list(event_data.keys())}")
        show("category (singular)", event_data.get("category"))
        show("categories (plural)", event_data.get("categories"))
        show("tags", event_data.get("tags"))

        # Show categories array detail
        event_categories = event_data.get("categories", [])
        if event_categories:
            print(f"         categories[0] (tipo {type(event_categories[0]).__name__}): {event_categories[0]}")
            cat_labels = [c.get("label") for c in event_categories if isinstance(c, dict)]
            print(f"         categories labels: {cat_labels}")

        event_tags = event_data.get("tags", [])
        if event_tags:
            print(f"         Primer tag (tipo {type(event_tags[0]).__name__}): {event_tags[0]}")

        # ============================================================
        # PASO 4: Simular merge (lo que hace _check_for_closures)
        # ============================================================
        header(4, "Simular merge en market dict")

        # Start from the fetch_market_by_id response
        market = market_by_id.copy()
        print(f"  Antes del merge:")
        show("  market['category']", market.get("category"), expect_truthy=False)

        # Apply the same logic as _check_for_closures
        event_category = event_data.get("category")
        if event_category:
            market["category"] = event_category
        event_tags_merge = event_data.get("tags", [])
        if event_tags_merge:
            if market.get("events") and len(market["events"]) > 0:
                market["events"][0]["tags"] = event_tags_merge

        print(f"  Despues del merge:")
        show("  market['category']", market.get("category"))
        show("  market['events'][0]['tags']", market.get("events", [{}])[0].get("tags"))

        # ============================================================
        # PASO 5: normalize_market() — RawMarketMetadata
        # ============================================================
        header(5, "normalize_market(market)")

        metadata = normalizer.normalize_market(market)

        show("metadata.category", metadata.category)
        show("metadata.tags", metadata.tags)
        print(f"         category type: {type(metadata.category)}")
        print(f"         tags type: {type(metadata.tags)}")

        # ============================================================
        # PASO 6: _market_to_dict() — lo que va a la BD
        # ============================================================
        header(6, "_market_to_dict(metadata) — dict para BD")

        # Replicate orchestrator._market_to_dict
        db_dict = {
            "market_id":      metadata.market_id,
            "question":       metadata.question,
            "end_date":       metadata.end_date,
            "closed":         metadata.closed,
            "outcome_prices": metadata.outcome_prices,
            "volume_usdc":    metadata.volume_usdc,
            "categories":     [metadata.category] if metadata.category else [],
            "tags":           metadata.tags or [],
            "winning_outcome": metadata.winning_outcome,
            "updated_at":     datetime.now(UTC),
        }

        show("categories", db_dict["categories"])
        show("tags", db_dict["tags"])
        print(f"         categories type: {type(db_dict['categories'])}, len: {len(db_dict['categories'])}")
        print(f"         tags type: {type(db_dict['tags'])}, len: {len(db_dict['tags'])}")

        # ============================================================
        # PASO 7: Consultar BD directamente
        # ============================================================
        header(7, "SELECT categories, tags FROM markets (BD)")

        try:
            from database.connection import async_session
            from sqlalchemy import text

            async with async_session() as session:
                # Check this specific market
                row = (await session.execute(
                    text("SELECT market_id, categories, tags FROM markets WHERE market_id = :mid"),
                    {"mid": condition_id}
                )).fetchone()

                if row:
                    print(f"  Market encontrado en BD:")
                    show("  categories", row[1])
                    show("  tags", row[2])
                else:
                    print(f"  Market {condition_id[:20]}... NO encontrado en BD")

                # Show last 5 markets
                print(f"\n  Ultimos 5 markets en BD:")
                rows = (await session.execute(
                    text("SELECT market_id, categories, tags FROM markets ORDER BY updated_at DESC LIMIT 5")
                )).fetchall()

                for r in rows:
                    cat_status = "✅" if r[1] and len(r[1]) > 0 else "❌"
                    tag_status = "✅" if r[2] and len(r[2]) > 0 else "❌"
                    print(f"    {r[0][:20]}...  categories {cat_status} {r[1]}  tags {tag_status} {r[2]}")

        except Exception as e:
            print(f"  ⚠️  No se pudo conectar a BD: {e}")

        # ============================================================
        # RESUMEN
        # ============================================================
        print(f"\n{'=' * 70}")
        print(f"  RESUMEN")
        print(f"{'=' * 70}")
        print(f"  API ?closed=true        → category: {closed_market.get('category')}")
        print(f"  API ?condition_ids=X     → category: {market_by_id.get('category')}")
        print(f"  Events API /events/{{id}} → category: {event_data.get('category')}")
        print(f"  Post-merge              → category: {market.get('category')}")
        print(f"  normalize_market()      → category: {metadata.category}")
        print(f"  _market_to_dict()       → categories: {db_dict['categories']}")
        print(f"  _market_to_dict()       → tags: {db_dict['tags']}")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
