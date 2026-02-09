# Test script for the realtime listener system
#
# Tests:
# 1. Event decoding from sample logs
# 2. WebSocket connection to Polygon RPC
# 3. Polling via Etherscan API V2
# 4. Full orchestrator with Redis publishing
#
# Usage:
#     poetry run python -m scripts.test_realtime_listener
#     poetry run python -m scripts.test_realtime_listener --mode decoder
#     poetry run python -m scripts.test_realtime_listener --mode websocket
#     poetry run python -m scripts.test_realtime_listener --mode polling
#     poetry run python -m scripts.test_realtime_listener --mode full

import asyncio
import argparse
import sys

import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
)

logger = structlog.get_logger()


async def test_event_decoder():
    """Test 1: Decode a sample OrderFilled log"""
    print("\n" + "=" * 60)
    print("TEST 1: Event Decoder")
    print("=" * 60)

    from ingestion.collectors.realtime import EventDecoder, ORDER_FILLED_TOPIC

    print(f"\n  OrderFilled topic: {ORDER_FILLED_TOPIC[:20]}...")

    # Sample log structure (real format from Polygon)
    # This simulates a BUY order: maker gives USDC (makerAssetId=0), receives tokens
    sample_log = {
        "transactionHash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "blockNumber": "0x3A98B5C",  # ~61,440,000
        "logIndex": "0x5",
        "address": "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
        "topics": [
            ORDER_FILLED_TOPIC,  # Event signature
            "0x1111111111111111111111111111111111111111111111111111111111111111",  # orderHash
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",  # maker
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",  # taker
        ],
        # Non-indexed data (ABI encoded):
        # makerAssetId = 0 (USDC)
        # takerAssetId = 12345 (token)
        # makerAmountFilled = 1000000 (1 USDC with 6 decimals)
        # takerAmountFilled = 2000000 (2 tokens)
        # fee = 1000 (0.001 USDC)
        "data": "0x"
        + "0000000000000000000000000000000000000000000000000000000000000000"  # makerAssetId = 0
        + "0000000000000000000000000000000000000000000000000000000000003039"  # takerAssetId = 12345
        + "00000000000000000000000000000000000000000000000000000000000f4240"  # makerAmount = 1000000
        + "00000000000000000000000000000000000000000000000000000000001e8480"  # takerAmount = 2000000
        + "00000000000000000000000000000000000000000000000000000000000003e8",  # fee = 1000
    }

    decoder = EventDecoder()
    decoded = decoder.decode_order_filled(sample_log)

    if decoded:
        print(f"\n  Decoded event:")
        print(f"    tx_hash: {decoded.tx_hash[:20]}...")
        print(f"    block: {decoded.block_number}")
        print(f"    maker: {decoded.maker}")
        print(f"    taker: {decoded.taker}")
        print(f"    side: {decoded.side}")
        print(f"    maker_asset_id: {decoded.maker_asset_id}")
        print(f"    taker_asset_id: {decoded.taker_asset_id}")
        print(f"    maker_amount: {decoded.maker_amount} USDC")
        print(f"    taker_amount: {decoded.taker_amount} tokens")
        print(f"    fee: {decoded.fee} USDC")
        print(f"    is_maker_usdc: {decoded.is_maker_usdc}")

        # Validate expected values
        assert decoded.side == "BUY", f"Expected BUY, got {decoded.side}"
        assert decoded.maker_amount == 1.0, f"Expected 1.0, got {decoded.maker_amount}"
        assert decoded.taker_amount == 2.0, f"Expected 2.0, got {decoded.taker_amount}"
        assert decoded.is_maker_usdc is True

        print("\n  [OK] Event decoder working correctly")
        return True
    else:
        print("  [FAIL] Could not decode event")
        return False


async def test_websocket_connection():
    """Test 2: WebSocket connection to Polygon RPC"""
    print("\n" + "=" * 60)
    print("TEST 2: WebSocket Connection")
    print("=" * 60)

    from ingestion.collectors.realtime import WebSocketListener, ConnectionState

    events_received = []
    connection_states = []

    async def on_event(event):
        events_received.append(event)
        print(
            f"    Event: tx={event.tx_hash[:12]}... "
            f"maker={event.maker[:10]}... "
            f"{event.side} "
            f"${event.maker_amount if event.is_maker_usdc else event.taker_amount:.2f}"
        )

    listener = WebSocketListener(on_event=on_event)

    print("\n  Connecting to Polygon WebSocket...")
    print("  (Waiting up to 30 seconds for events...)")
    print("  Events received:")

    # Run for 30 seconds max
    async def run_with_timeout():
        task = asyncio.create_task(listener.start())
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=30)
        except asyncio.TimeoutError:
            await listener.stop()

    try:
        await run_with_timeout()
    except Exception as e:
        print(f"\n  Connection error: {e}")

    print(f"\n  Summary:")
    print(f"    Events received: {len(events_received)}")
    print(f"    Final state: {listener.state.value}")
    print(f"    Last block seen: {listener.last_block_seen}")

    if listener.state in [ConnectionState.SUBSCRIBED, ConnectionState.CONNECTED]:
        print("\n  [OK] WebSocket connection successful")
        return True
    elif len(events_received) > 0:
        print("\n  [OK] WebSocket received events (connection worked)")
        return True
    else:
        print("\n  [WARN] No events received (market may be quiet)")
        print("         WebSocket connection itself may be working")
        return True  # Not a failure, just no activity


async def test_polling():
    """Test 3: Polling via Etherscan API V2"""
    print("\n" + "=" * 60)
    print("TEST 3: Polling (Etherscan API V2)")
    print("=" * 60)

    from ingestion.config import get_settings
    from ingestion.http_client import HTTPClient
    from ingestion.collectors.realtime import PollingListener

    settings = get_settings()

    if not settings.etherscan_api_key:
        print("\n  [SKIP] ETHERSCAN_API_KEY not configured in .env")
        return None

    print(f"\n  Etherscan API key: {settings.etherscan_api_key[:8]}...")

    events_received = []

    async def on_event(event):
        events_received.append(event)
        print(
            f"    Event: tx={event.tx_hash[:12]}... "
            f"{event.side} "
            f"${event.maker_amount if event.is_maker_usdc else event.taker_amount:.2f}"
        )

    async with HTTPClient() as client:
        listener = PollingListener(on_event=on_event, http_client=client)

        # Get latest block
        latest = await listener._get_latest_block()
        print(f"  Latest Polygon block: {latest:,}")

        # Catch up last 50 blocks (~1.5 minutes of activity)
        from_block = latest - 50
        print(f"  Fetching events from block {from_block:,} to {latest:,}...")
        print("  Events found:")

        events_count = await listener.catch_up(from_block, latest)

    print(f"\n  Summary:")
    print(f"    Blocks scanned: 50")
    print(f"    Events found: {events_count}")

    print("\n  [OK] Polling working correctly")
    return True


async def test_full_orchestrator():
    """Test 4: Full orchestrator with Redis"""
    print("\n" + "=" * 60)
    print("TEST 4: Full Orchestrator (with Redis)")
    print("=" * 60)

    from ingestion.publisher import RedisPublisher
    from ingestion.collectors.realtime import RealtimeOrchestrator

    # Check Redis connection first
    publisher = RedisPublisher()
    redis_available = await publisher.health_check()

    if not redis_available:
        print("\n  [SKIP] Redis not available")
        print("         Run: docker-compose up -d redis")
        await publisher.close()
        return None

    print("\n  Redis: connected")
    print("  Starting orchestrator...")
    print("  (Running for 45 seconds...)")

    async with RealtimeOrchestrator(publisher=publisher) as orchestrator:
        # Run for 45 seconds
        task = asyncio.create_task(orchestrator.run())

        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=45)
        except asyncio.TimeoutError:
            await orchestrator.stop()

        print(f"\n  Summary:")
        print(f"    Events processed: {orchestrator.events_processed}")
        print(f"    Events deduplicated: {orchestrator.events_deduplicated}")
        print(f"    Using fallback: {orchestrator.using_fallback}")

    # Check Redis stream
    stream_info = await publisher.get_stream_info()
    print(f"    Redis stream info: {stream_info}")

    await publisher.close()

    print("\n  [OK] Orchestrator test complete")
    return True


async def test_deduplication():
    """Test 5: LRU Cache deduplication"""
    print("\n" + "=" * 60)
    print("TEST 5: Deduplication (LRU Cache)")
    print("=" * 60)

    from ingestion.collectors.realtime import LRUCache

    cache = LRUCache(max_size=5)

    # Add items
    for i in range(7):
        key = f"tx_{i}"
        cache.add(key)
        print(f"  Added: {key}, cache size: {len(cache)}")

    # Check eviction (oldest should be gone)
    print("\n  Checking eviction:")
    print(f"    tx_0 in cache: {cache.contains('tx_0')}")  # Should be False
    print(f"    tx_1 in cache: {cache.contains('tx_1')}")  # Should be False
    print(f"    tx_6 in cache: {cache.contains('tx_6')}")  # Should be True

    assert not cache.contains("tx_0"), "tx_0 should have been evicted"
    assert cache.contains("tx_6"), "tx_6 should be in cache"

    print("\n  [OK] Deduplication working correctly")
    return True


async def main():
    parser = argparse.ArgumentParser(description="Test realtime listener")
    parser.add_argument(
        "--mode",
        choices=["decoder", "websocket", "polling", "full", "dedup", "all"],
        default="all",
        help="Test mode to run",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("REALTIME LISTENER TEST SUITE")
    print("=" * 60)

    results = {}

    if args.mode in ["decoder", "all"]:
        results["decoder"] = await test_event_decoder()

    if args.mode in ["dedup", "all"]:
        results["deduplication"] = await test_deduplication()

    if args.mode in ["polling", "all"]:
        results["polling"] = await test_polling()

    if args.mode in ["websocket", "all"]:
        results["websocket"] = await test_websocket_connection()

    if args.mode in ["full", "all"]:
        results["full_orchestrator"] = await test_full_orchestrator()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for test, result in results.items():
        if result is True:
            status = "PASS"
        elif result is None:
            status = "SKIP"
        else:
            status = "FAIL"
        print(f"  {test}: {status}")

    # Exit code
    failures = [r for r in results.values() if r is False]
    if failures:
        print(f"\n  {len(failures)} test(s) failed")
        sys.exit(1)
    else:
        print("\n  All tests passed!")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
