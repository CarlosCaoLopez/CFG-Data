# Test Ingestion → Queue Integration
# Verifica que el publisher de ingestion se conecta al queue_system
# Usage: python -m scripts.test_ingestion_to_queue
# Requires: docker-compose up -d redis

import asyncio
from datetime import datetime, UTC, timezone

import redis.asyncio as redis

from contracts.raw_market_data import (
    MarketDataBatch,
    RawMarketMetadata,
    RawTrade,
    RawWalletAge,
)
from queue_system.config import QueueConfig
from queue_system.streams.raw_data_stream import RawDataStream


async def main():
    """Test that ingestion can publish to queue_system"""

    print("=" * 60)
    print("Test: Ingestion → Queue System Integration")
    print("=" * 60)

    # Step 1: Connect to Redis
    print("\n[1] Connecting to Redis...")

    config = QueueConfig()
    print(f"    Redis URL: {config.redis_url}")
    print(f"    Stream: {config.raw_data_stream_name}")

    try:
        # Note: Do NOT use decode_responses=True here
        # RawDataStream expects bytes for XREADGROUP responses
        redis_client = redis.from_url(config.redis_url)
        await redis_client.ping()
        print("    ✓ Redis connection OK")
    except Exception as e:
        print(f"    ✗ Redis connection FAILED: {e}")
        print("\n    Run: docker-compose up -d redis")
        return

    # Step 2: Create RawDataStream (queue_system)
    print("\n[2] Creating RawDataStream...")

    raw_data_stream = RawDataStream(redis_client, config.raw_data_stream_name)

    # Get initial stream length
    initial_length = await raw_data_stream.get_stream_length()
    print(f"    Stream length (before): {initial_length}")

    # Step 3: Create a sample MarketDataBatch
    print("\n[3] Creating sample MarketDataBatch...")

    sample_batch = MarketDataBatch(
        metadata=RawMarketMetadata(
            market_id="0xtest1234567890abcdef",
            question="Test market for integration testing?",
            end_date=datetime(2026, 2, 15, 0, 0, 0, tzinfo=timezone.utc),
            closed=True,
            outcome_prices=[1.0, 0.0],
            volume_usdc=50000.0,
        ),
        trades=[
            RawTrade(
                tx_hash="0xabc123def456789test",
                wallet="0x1234567890abcdef1234567890abcdef12345678",
                market_id="0xtest1234567890abcdef",
                timestamp=datetime(2026, 2, 14, 22, 30, 0, tzinfo=timezone.utc),
                side="BUY",
                outcome="Yes",
                price=0.65,
                size_usdc=1000.0,
                size_tokens=1538.46,
            ),
        ],
        wallet_ages={
            "0x1234567890abcdef1234567890abcdef12345678": RawWalletAge(
                wallet="0x1234567890abcdef1234567890abcdef12345678",
                first_tx_polymarket=datetime(2026, 2, 14, 20, 0, 0, tzinfo=timezone.utc),
                first_tx_polygon=datetime(2026, 2, 14, 19, 45, 0, tzinfo=timezone.utc),
            )
        },
        ingested_at=datetime.now(UTC),
    )

    print(f"    Market: {sample_batch.metadata.question[:40]}...")
    print(f"    Trades: {len(sample_batch.trades)}")
    print(f"    Wallets: {len(sample_batch.wallet_ages)}")

    # Step 4: Publish to stream
    print("\n[4] Publishing to RawDataStream...")

    try:
        message_id = await raw_data_stream.add(sample_batch)
        print(f"    ✓ Published with message_id: {message_id}")
    except Exception as e:
        print(f"    ✗ Publish FAILED: {e}")
        await redis_client.close()
        return

    # Step 5: Verify publication
    print("\n[5] Verifying publication...")

    final_length = await raw_data_stream.get_stream_length()
    print(f"    Stream length (after): {final_length}")

    if final_length > initial_length:
        print("    ✓ Message successfully added to stream!")
    else:
        print("    ✗ Message NOT found in stream")

    # Step 6: Create consumer group (simulating queue_system setup)
    print("\n[6] Creating consumer group (for orchestrator)...")

    try:
        created = await raw_data_stream.create_consumer_group("scheduler")
        if created:
            print("    ✓ Consumer group 'scheduler' created")
        else:
            print("    ○ Consumer group 'scheduler' already exists")
    except Exception as e:
        print(f"    ✗ Consumer group creation failed: {e}")

    # Step 7: Read message back (simulating orchestrator)
    print("\n[7] Reading message back (simulating orchestrator)...")

    try:
        messages = await raw_data_stream.read(
            consumer_group="scheduler",
            consumer_name="test-consumer",
            count=1,
            block_ms=1000,
        )

        if messages:
            msg_id, batch = messages[0]
            print(f"    ✓ Message received: {msg_id}")
            print(f"      Market: {batch.metadata.market_id[:20]}...")
            print(f"      Trades: {len(batch.trades)}")

            # ACK the message
            await raw_data_stream.ack("scheduler", msg_id)
            print("    ✓ Message acknowledged")
        else:
            print("    ○ No new messages (might be already consumed)")
    except Exception as e:
        print(f"    ✗ Read failed: {e}")

    # Cleanup
    await redis_client.aclose()

    print("\n" + "=" * 60)
    print("Integration test completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
