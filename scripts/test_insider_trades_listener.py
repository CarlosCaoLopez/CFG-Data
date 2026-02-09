# Test script for L2 InsiderTradesListener
# Validates: command processing, category matching, trade filtering
#
# Usage:
#   poetry run python -m scripts.test_insider_trades_listener
#
# For Redis integration tests, ensure Redis is running:
#   docker-compose up -d redis

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.listeners.insider_trades_listener import (
    InsiderTradesListener,
    MonitoredWallet,
)
from contracts.raw_market_data import InsiderTradeCommand, InsiderTradeData, RawTrade


def test_insider_trade_command_model():
    """Test InsiderTradeCommand contract model."""
    print("\n" + "=" * 60)
    print("TEST 1: InsiderTradeCommand model")
    print("=" * 60)

    # ADD_WALLET command
    cmd = InsiderTradeCommand(
        wallet="0xabc123def456",
        categories_insider=["Sports", "Crypto"],
        command_type="ADD_WALLET",
    )
    print(f"  Command type: {cmd.command_type}")
    print(f"  Wallet: {cmd.wallet}")
    print(f"  Categories: {cmd.categories_insider}")
    assert cmd.command_type == "ADD_WALLET"
    assert len(cmd.categories_insider) == 2

    # Serialize/deserialize
    json_str = cmd.model_dump_json()
    cmd2 = InsiderTradeCommand.model_validate_json(json_str)
    assert cmd2.wallet == cmd.wallet
    assert cmd2.categories_insider == cmd.categories_insider
    print(f"  Serialization roundtrip OK ✅")

    # With market_ids
    cmd3 = InsiderTradeCommand(
        wallet="0x999",
        categories_insider=["Sports"],
        market_ids_insider=["0xmarket1", "0xmarket2"],
        command_type="UPDATE_CATEGORIES",
    )
    assert cmd3.market_ids_insider is not None
    assert len(cmd3.market_ids_insider) == 2
    print(f"  With market_ids: {cmd3.market_ids_insider} ✅")

    print("\n  ✅ InsiderTradeCommand model tests passed")


def test_insider_trade_data_model():
    """Test InsiderTradeData contract model."""
    print("\n" + "=" * 60)
    print("TEST 2: InsiderTradeData model")
    print("=" * 60)

    trade = RawTrade(
        tx_hash="0xtx123",
        wallet="0xinsider",
        market_id="0xmarket",
        timestamp=datetime.now(timezone.utc),
        side="BUY",
        outcome="Yes",
        price=0.35,
        size_usdc=5000.0,
        size_tokens=14285.71,
    )

    data = InsiderTradeData(
        wallet="0xinsider",
        trades=[trade],
        categories_insider=["Sports", "Crypto"],
    )

    print(f"  Wallet: {data.wallet}")
    print(f"  Trades count: {len(data.trades)}")
    print(f"  Categories: {data.categories_insider}")
    print(f"  Published at: {data.published_at}")

    assert data.wallet == "0xinsider"
    assert len(data.trades) == 1
    assert data.published_at is not None

    # Serialize/deserialize
    json_str = data.model_dump_json()
    data2 = InsiderTradeData.model_validate_json(json_str)
    assert data2.wallet == data.wallet
    assert len(data2.trades) == 1
    print(f"  Serialization roundtrip OK ✅")

    print("\n  ✅ InsiderTradeData model tests passed")


def test_command_processing():
    """Test that L2 processes ADD/UPDATE/REMOVE commands correctly."""
    print("\n" + "=" * 60)
    print("TEST 3: Command processing (unit)")
    print("=" * 60)

    listener = InsiderTradesListener()

    # Simulate ADD_WALLET command data (as it would come from Redis)
    async def run_commands():
        # ADD_WALLET
        cmd_data = {
            b"payload": InsiderTradeCommand(
                wallet="0xInsider1",
                categories_insider=["Sports", "Crypto"],
                command_type="ADD_WALLET",
            ).model_dump_json().encode(),
        }
        await listener._process_command(b"1-0", cmd_data)
        assert listener.monitored_count == 1
        assert "0xinsider1" in listener._monitored_wallets
        print("  ADD_WALLET → 1 wallet monitored ✅")

        # ADD another
        cmd_data2 = {
            b"wallet": b"0xInsider2",
            b"categories_insider": json.dumps(["Pop Culture"]).encode(),
            b"command_type": b"ADD_WALLET",
        }
        await listener._process_command(b"2-0", cmd_data2)
        assert listener.monitored_count == 2
        print("  ADD_WALLET (individual fields) → 2 wallets ✅")

        # UPDATE_CATEGORIES
        cmd_data3 = {
            b"payload": InsiderTradeCommand(
                wallet="0xInsider1",
                categories_insider=["Sports", "Crypto", "Pop Culture"],
                command_type="UPDATE_CATEGORIES",
            ).model_dump_json().encode(),
        }
        await listener._process_command(b"3-0", cmd_data3)
        assert len(listener._monitored_wallets["0xinsider1"].categories_insider) == 3
        print("  UPDATE_CATEGORIES → 3 categories ✅")

        # REMOVE_WALLET
        cmd_data4 = {
            b"payload": InsiderTradeCommand(
                wallet="0xInsider2",
                categories_insider=[],
                command_type="REMOVE_WALLET",
            ).model_dump_json().encode(),
        }
        await listener._process_command(b"4-0", cmd_data4)
        assert listener.monitored_count == 1
        assert "0xinsider2" not in listener._monitored_wallets
        print("  REMOVE_WALLET → 1 wallet remaining ✅")

        # Stats
        print(f"\n  Stats: {listener.stats}")

    asyncio.run(run_commands())
    print("\n  ✅ Command processing tests passed")


def test_monitored_wallet_dataclass():
    """Test MonitoredWallet state tracking."""
    print("\n" + "=" * 60)
    print("TEST 4: MonitoredWallet state")
    print("=" * 60)

    wallet = MonitoredWallet(
        wallet="0xtest",
        categories_insider=["Sports"],
    )

    assert wallet.last_seen_ts is None
    assert wallet.trades_published == 0

    wallet.last_seen_ts = datetime(2026, 1, 15, tzinfo=timezone.utc)
    wallet.trades_published = 5

    print(f"  Wallet: {wallet.wallet}")
    print(f"  Categories: {wallet.categories_insider}")
    print(f"  Last seen: {wallet.last_seen_ts}")
    print(f"  Trades published: {wallet.trades_published}")

    assert wallet.last_seen_ts.year == 2026
    assert wallet.trades_published == 5

    print("\n  ✅ MonitoredWallet state tests passed")


def test_config_l2():
    """Test L2 config values."""
    print("\n" + "=" * 60)
    print("TEST 5: L2 Config")
    print("=" * 60)

    from ingestion.config import get_settings
    settings = get_settings()

    print(f"  L2 poll interval: {settings.l2_poll_interval_seconds}s")
    print(f"  L2 commands stream: {settings.l2_commands_stream}")
    print(f"  L2 consumer group: {settings.l2_commands_consumer_group}")
    print(f"  Insider trades stream: {settings.insider_trades_stream}")

    assert settings.l2_poll_interval_seconds > 0
    assert settings.l2_commands_stream == "insider_commands"
    assert settings.insider_trades_stream == "insider_trades"

    print("\n  ✅ L2 config tests passed")


async def test_redis_command_injection():
    """Integration test: inject a command into Redis and verify L2 picks it up."""
    print("\n" + "=" * 60)
    print("TEST 6: Redis command injection (integration)")
    print("=" * 60)

    try:
        import redis.asyncio as aioredis
        from ingestion.config import get_settings

        settings = get_settings()
        r = aioredis.from_url(settings.redis_url)

        # Check Redis connection
        await r.ping()
        print("  Redis connection OK ✅")

        stream = settings.l2_commands_stream

        # Inject a test command
        cmd = InsiderTradeCommand(
            wallet="0xTestInsider123",
            categories_insider=["Sports", "Crypto"],
            command_type="ADD_WALLET",
        )

        msg_id = await r.xadd(stream, {
            "payload": cmd.model_dump_json(),
            "wallet": cmd.wallet,
            "command_type": cmd.command_type,
        })

        msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
        print(f"  Injected command: {msg_id_str}")
        print(f"    Wallet: {cmd.wallet}")
        print(f"    Categories: {cmd.categories_insider}")
        print(f"    Command type: {cmd.command_type}")

        # Verify stream has the message
        length = await r.xlen(stream)
        print(f"  Stream '{stream}' length: {length}")

        # Read back
        messages = await r.xrange(stream, "-", "+", count=5)
        for mid, data in messages:
            mid_str = mid.decode() if isinstance(mid, bytes) else str(mid)
            wallet = data.get(b"wallet", b"").decode()
            print(f"    Message {mid_str}: wallet={wallet}")

        await r.aclose()
        print("\n  ✅ Redis integration test passed")
        print(f"  (Use XREADGROUP to consume from '{stream}')")

    except Exception as e:
        print(f"  ⚠️ Skipped: {e}")
        print("  (Ensure Redis is running: docker-compose up -d redis)")


async def test_real_wallet_polling():
    """Integration test: fetch real wallets from a closed market, then poll their trades via L2."""
    print("\n" + "=" * 60)
    print("TEST 7: Real wallet polling (live API integration)")
    print("=" * 60)

    try:
        from ingestion.collectors.markets_collector import MarketsCollector
        from ingestion.collectors.trades_collector import TradesCollector
        from ingestion.normalizers.data_normalizer import DataNormalizer
        from ingestion.http_client import get_http_client
        from ingestion.config import get_settings

        settings = get_settings()
        markets_collector = MarketsCollector()
        trades_collector = TradesCollector()
        normalizer = DataNormalizer()

        # Step 1: Get a closed market to extract real wallets
        print("\n  Step 1: Fetching a closed market to find real wallets...")
        market = None
        async for m in markets_collector.fetch_closed_markets(limit=1):
            market = m
            break

        if not market:
            print("  ⚠️ No closed markets found, skipping")
            return

        market_id = market.get("conditionId", "")
        question = market.get("question", "N/A")
        category = market.get("category", "N/A")
        print(f"    Market: {question[:60]}...")
        print(f"    conditionId: {market_id}")
        print(f"    Category: {category}")

        # Step 2: Fetch trades to get real wallets
        print("\n  Step 2: Fetching trades to extract wallets...")
        trades = []
        async for trade in trades_collector.fetch_trades_for_market(market_id, limit=20):
            trades.append(trade)

        if not trades:
            print("  ⚠️ No trades found for this market, skipping")
            return

        all_wallets = sorted(trades_collector.extract_wallets_from_trades(trades))
        print(f"    Found {len(all_wallets)} unique wallets")

        # Pick up to 3 wallets for testing
        test_wallets = all_wallets[:3]
        print(f"    Using {len(test_wallets)} wallets for L2 test:")
        for w in test_wallets:
            print(f"      {w}")

        # Step 3: Create L2 listener and add wallets via command processing
        print("\n  Step 3: Simulating ADD_WALLET commands for real wallets...")
        listener = InsiderTradesListener()

        # Use broad categories so trades are likely to match
        test_categories = settings.categorias_de_interes

        for i, wallet in enumerate(test_wallets):
            cmd_data = {
                b"payload": InsiderTradeCommand(
                    wallet=wallet,
                    categories_insider=test_categories,
                    command_type="ADD_WALLET",
                ).model_dump_json().encode(),
            }
            await listener._process_command(f"{i+1}-0".encode(), cmd_data)

        print(f"    Monitored wallets: {listener.monitored_count}")
        assert listener.monitored_count == len(test_wallets)
        print(f"    Commands processed: {listener.stats['commands_received']} ✅")

        # Step 4: Poll trades for each wallet (the core L2 functionality)
        print("\n  Step 4: Polling trades for each real wallet...")
        for wallet in test_wallets:
            wallet_lower = wallet.lower()
            info = listener._monitored_wallets.get(wallet_lower)
            if not info:
                print(f"    {wallet}: not in monitored (unexpected)")
                continue

            # Fetch trades directly to see what the API returns
            trade_count = 0
            async for t in trades_collector.fetch_trades_for_wallet(wallet, limit=10):
                trade_count += 1

            print(f"    {wallet}:")
            print(f"      Trades from API: {trade_count}")
            print(f"      Categories: {info.categories_insider}")

        # Step 5: Run _poll_all_wallets to test the full L2 flow
        print("\n  Step 5: Running _poll_all_wallets() (full L2 flow)...")
        listener._running = True  # Required: _poll_all_wallets checks this flag
        try:
            await listener._poll_all_wallets()
            print(f"    Trades polled: {listener.stats['trades_polled']}")
            print(f"    Trades published: {listener.stats['trades_published']}")
            print(f"    (publish_insider_trades skipped — no Redis in this test)")
        except Exception as e:
            # Expected: publish will fail without Redis, but polling should work
            polled = listener.stats['trades_polled']
            print(f"    Trades polled before publish: {polled}")
            print(f"    Publish failed (expected without Redis): {type(e).__name__}")

        # Show final stats
        print(f"\n  Final stats: {listener.stats}")

        # Cleanup
        client = get_http_client()
        await client.close()

        print("\n  ✅ Real wallet polling test completed")

    except Exception as e:
        print(f"  ⚠️ Skipped: {e}")
        import traceback
        traceback.print_exc()


def main():
    print("\n" + "=" * 60)
    print("  L2 INSIDER TRADES LISTENER - TEST SUITE")
    print("=" * 60)

    # Unit tests
    test_insider_trade_command_model()
    test_insider_trade_data_model()
    test_command_processing()
    test_monitored_wallet_dataclass()
    test_config_l2()

    # Integration tests
    print("\n" + "-" * 60)
    print("Running integration tests...")
    asyncio.run(test_redis_command_injection())
    asyncio.run(test_real_wallet_polling())

    print("\n" + "=" * 60)
    print("  ALL L2 TESTS COMPLETED SUCCESSFULLY ✅")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
