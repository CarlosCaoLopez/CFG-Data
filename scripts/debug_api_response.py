# Debug script to inspect raw API responses for category field
# Run: poetry run python -m scripts.debug_api_response

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.http_client import get_http_client, APIType
from ingestion.config import get_settings


async def main():
    settings = get_settings()
    client = get_http_client()

    # Test condition_id from the real-time test
    condition_id = "0xc6fb98582841ddf56bc0e99f450c0059ed84ebb61e3c0b418074ac283ec7571f"
    token_id = "39784689153225151233663822041454016365250457952184354071400446743628998024353"

    print("=" * 60)
    print("DEBUG: Inspecting Gamma API responses for category")
    print("=" * 60)

    # 1. Query by clob_token_ids - get event_id
    print("\n1. GET /markets?clob_token_ids=... (get event_id)")
    print("-" * 40)
    response1 = await client.get(
        f"{settings.gamma_api_url}/markets",
        params={"clob_token_ids": token_id},
        api_type=APIType.POLYMARKET,
    )

    event_id = None
    if isinstance(response1, list) and response1:
        market1 = response1[0]
        print(f"   category: {market1.get('category')}")
        events1 = market1.get("events", [])
        if events1:
            event_id = events1[0].get("id")
            series_slug = events1[0].get("seriesSlug")
            series = events1[0].get("series", [])
            print(f"   events[0].id: {event_id}")
            print(f"   events[0].seriesSlug: {series_slug}")
            if series:
                print(f"   events[0].series[0].title: {series[0].get('title')}")
                print(f"   events[0].series[0].slug: {series[0].get('slug')}")

    # 2. Query Events API directly (THE KEY TEST)
    if event_id:
        print(f"\n2. GET /events/{event_id} (Events API)")
        print("-" * 40)
        try:
            response_event = await client.get(
                f"{settings.gamma_api_url}/events/{event_id}",
                api_type=APIType.POLYMARKET,
            )

            if isinstance(response_event, dict):
                print(f"   Keys: {list(response_event.keys())}")
                print(f"   category: {response_event.get('category')}")
                print(f"   tags: {response_event.get('tags')}")
                print(f"   seriesSlug: {response_event.get('seriesSlug')}")
                series = response_event.get("series", [])
                if series:
                    print(f"   series[0].title: {series[0].get('title')}")
            elif isinstance(response_event, list) and response_event:
                event_data = response_event[0]
                print(f"   Keys: {list(event_data.keys())}")
                print(f"   category: {event_data.get('category')}")
                print(f"   tags: {event_data.get('tags')}")
            else:
                print(f"   Response type: {type(response_event)}")
                print(f"   Response: {str(response_event)[:500]}")
        except Exception as e:
            print(f"   ERROR: {e}")

    # 3. Query Events API with slug (alternative)
    if isinstance(response1, list) and response1:
        events1 = response1[0].get("events", [])
        if events1:
            event_slug = events1[0].get("slug")
            if event_slug:
                print(f"\n3. GET /events?slug={event_slug}")
                print("-" * 40)
                try:
                    response_slug = await client.get(
                        f"{settings.gamma_api_url}/events",
                        params={"slug": event_slug},
                        api_type=APIType.POLYMARKET,
                    )
                    if isinstance(response_slug, list) and response_slug:
                        evt = response_slug[0]
                        print(f"   Keys: {list(evt.keys())}")
                        print(f"   category: {evt.get('category')}")
                        print(f"   tags: {evt.get('tags')}")
                    elif isinstance(response_slug, dict):
                        print(f"   Keys: {list(response_slug.keys())}")
                        print(f"   category: {response_slug.get('category')}")
                except Exception as e:
                    print(f"   ERROR: {e}")

    # 4. Closed market comparison (we know this has category)
    print("\n4. GET /markets?closed=true&limit=1 (comparison)")
    print("-" * 40)
    response3 = await client.get(
        f"{settings.gamma_api_url}/markets",
        params={"closed": "true", "limit": "1"},
        api_type=APIType.POLYMARKET,
    )

    if isinstance(response3, list) and response3:
        market3 = response3[0]
        print(f"   category: {market3.get('category')}")
        events3 = market3.get("events", [])
        if events3:
            closed_event_id = events3[0].get("id")
            print(f"   events[0].category: {events3[0].get('category')}")
            print(f"   events[0].tags: {events3[0].get('tags')}")

            # Also try Events API for the closed market event
            if closed_event_id:
                print(f"\n5. GET /events/{closed_event_id} (closed market event)")
                print("-" * 40)
                try:
                    response_closed_event = await client.get(
                        f"{settings.gamma_api_url}/events/{closed_event_id}",
                        api_type=APIType.POLYMARKET,
                    )
                    if isinstance(response_closed_event, dict):
                        print(f"   Keys: {list(response_closed_event.keys())}")
                        print(f"   category: {response_closed_event.get('category')}")
                        print(f"   tags: {response_closed_event.get('tags')}")
                    elif isinstance(response_closed_event, list) and response_closed_event:
                        evt = response_closed_event[0]
                        print(f"   Keys: {list(evt.keys())}")
                        print(f"   category: {evt.get('category')}")
                        print(f"   tags: {evt.get('tags')}")
                except Exception as e:
                    print(f"   ERROR: {e}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("If Events API returns category, we can use:")
    print("  1. clob_token_ids → get event_id")
    print("  2. /events/{event_id} → get category")
    print("If not, fallback to seriesSlug as category proxy")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
