"""
Integration tests for the insider detection flow:
  API endpoint → DB upsert → Celery T3 → Redis stream

Requires: PostgreSQL on localhost:5433 and Redis on localhost:6379
Run:      pytest tests/test_insider_detection.py -v
"""
import json

import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import text

from tests.conftest import make_batch, make_wallet


# ═══════════════════════════════════════════════════════════════════
# 1. Pydantic contract tests
# ═══════════════════════════════════════════════════════════════════

class TestInsiderTradeCommand:
    def test_serialize_full(self):
        from contracts.raw_market_data import InsiderTradeCommand

        cmd = InsiderTradeCommand(
            wallet="0xaaa",
            categories_insider=["Crypto", "Politics"],
            market_ids_insider=["0xmarket1"],
            command_type="ADD_WALLET",
        )
        data = cmd.model_dump()
        assert data["wallet"] == "0xaaa"
        assert data["categories_insider"] == ["Crypto", "Politics"]
        assert data["market_ids_insider"] == ["0xmarket1"]
        assert data["command_type"] == "ADD_WALLET"

    def test_serialize_no_market_ids(self):
        from contracts.raw_market_data import InsiderTradeCommand

        cmd = InsiderTradeCommand(
            wallet="0xbbb",
            categories_insider=["Sports"],
            command_type="UPDATE_CATEGORIES",
        )
        assert cmd.market_ids_insider is None

    def test_roundtrip_json(self):
        from contracts.raw_market_data import InsiderTradeCommand

        cmd = InsiderTradeCommand(
            wallet="0xccc",
            categories_insider=["Crypto"],
            command_type="REMOVE_WALLET",
        )
        json_str = cmd.model_dump_json()
        restored = InsiderTradeCommand.model_validate_json(json_str)
        assert restored == cmd

    def test_invalid_command_type(self):
        from contracts.raw_market_data import InsiderTradeCommand
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            InsiderTradeCommand(
                wallet="0xddd",
                categories_insider=["Crypto"],
                command_type="INVALID_TYPE",
            )


class TestAPISchemas:
    def test_batch_requires_wallets(self):
        from api.main import InsiderDetectionBatch
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            InsiderDetectionBatch(market_id="0xabc", wallets=[])

    def test_batch_valid(self):
        from api.main import InsiderDetectionBatch

        batch = InsiderDetectionBatch(
            market_id="0xabc",
            wallets=[{"wallet": "0x111", "categories_insider": ["Crypto"]}],
        )
        assert len(batch.wallets) == 1
        assert batch.wallets[0].command_type == "ADD_WALLET"  # default

    def test_command_type_default(self):
        from api.main import InsiderWalletResult

        w = InsiderWalletResult(wallet="0x111", categories_insider=["Crypto"])
        assert w.command_type == "ADD_WALLET"


# ═══════════════════════════════════════════════════════════════════
# 2. API endpoint tests (mock Celery, real DB)
# ═══════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestInsiderDetectionEndpoint:

    async def test_accepted_single_wallet(self, api_client, seed_market, clean_wallets):
        body = make_batch(seed_market, [
            make_wallet(clean_wallets[0], ["Crypto"]),
        ])
        with patch("workers.tasks.publish_insider_command") as mock_task:
            mock_task.delay = MagicMock()
            resp = await api_client.post("/insider-detection", json=body)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "accepted"
        assert data["wallets_updated"] == 1
        assert data["tasks_dispatched"] == 1
        assert data["errors"] is None

    async def test_accepted_multiple_wallets(self, api_client, seed_market, clean_wallets):
        body = make_batch(seed_market, [
            make_wallet(clean_wallets[0], ["Crypto", "Finance"]),
            make_wallet(clean_wallets[1], ["Politics"]),
            make_wallet(clean_wallets[2], ["Sports"]),
        ])
        with patch("workers.tasks.publish_insider_command") as mock_task:
            mock_task.delay = MagicMock()
            resp = await api_client.post("/insider-detection", json=body)

        data = resp.json()
        assert data["status"] == "accepted"
        assert data["wallets_updated"] == 3
        assert data["tasks_dispatched"] == 3

    async def test_wallet_persisted_in_db(self, api_client, db_session, seed_market, clean_wallets):
        body = make_batch(seed_market, [
            make_wallet(clean_wallets[0], ["Crypto", "Finance"]),
        ])
        with patch("workers.tasks.publish_insider_command") as mock_task:
            mock_task.delay = MagicMock()
            await api_client.post("/insider-detection", json=body)

        row = (await db_session.execute(
            text("SELECT insider, categories_insider, marketids_insider FROM wallets WHERE address = :addr"),
            {"addr": clean_wallets[0]},
        )).fetchone()

        assert row is not None
        assert row[0] is True  # insider
        assert set(row[1]) == {"Crypto", "Finance"}  # categories
        assert seed_market in row[2]  # marketids

    async def test_array_accumulation(self, api_client, db_session, seed_market, clean_wallets):
        """Calling twice with different categories should accumulate, not replace."""
        wallet_addr = clean_wallets[0]

        with patch("workers.tasks.publish_insider_command") as mock_task:
            mock_task.delay = MagicMock()
            # First call: Crypto
            await api_client.post("/insider-detection", json=make_batch(
                seed_market, [make_wallet(wallet_addr, ["Crypto"])]
            ))
            # Second call: Politics (same market, new category)
            await api_client.post("/insider-detection", json=make_batch(
                seed_market, [make_wallet(wallet_addr, ["Politics"])]
            ))

        row = (await db_session.execute(
            text("SELECT categories_insider FROM wallets WHERE address = :addr"),
            {"addr": wallet_addr},
        )).fetchone()

        assert row is not None
        assert "Crypto" in row[0]
        assert "Politics" in row[0]

    async def test_t3_dispatch_payload(self, api_client, seed_market, clean_wallets):
        """Verify the exact payload dispatched to Celery T3."""
        with patch("workers.tasks.publish_insider_command") as mock_task:
            mock_task.delay = MagicMock()
            await api_client.post("/insider-detection", json=make_batch(
                seed_market, [make_wallet(clean_wallets[0], ["Crypto"], "ADD_WALLET")]
            ))

        mock_task.delay.assert_called_once()
        call_arg = mock_task.delay.call_args[0][0]
        assert call_arg["wallet"] == clean_wallets[0]
        assert call_arg["categories_insider"] == ["Crypto"]
        assert call_arg["market_ids_insider"] == [seed_market]
        assert call_arg["command_type"] == "ADD_WALLET"

    async def test_empty_wallets_rejected(self, api_client):
        resp = await api_client.post("/insider-detection", json={
            "market_id": "0xabc",
            "wallets": [],
        })
        assert resp.status_code == 422  # Pydantic validation error

    async def test_missing_market_id_rejected(self, api_client):
        resp = await api_client.post("/insider-detection", json={
            "wallets": [make_wallet("0xaaa", ["Crypto"])],
        })
        assert resp.status_code == 422


# ═══════════════════════════════════════════════════════════════════
# 3. upsert_wallet array accumulation (direct DB tests)
# ═══════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestUpsertWalletAccumulation:

    async def test_insert_new_wallet(self, db_session, clean_wallets):
        from database.manager import DatabaseManager

        db = DatabaseManager(db_session)
        await db.upsert_wallet({
            "address": clean_wallets[0],
            "insider": True,
            "categories_insider": ["Crypto"],
            "marketids_insider": ["0xmarket_A"],
        })

        row = (await db_session.execute(
            text("SELECT insider, categories_insider, marketids_insider FROM wallets WHERE address = :addr"),
            {"addr": clean_wallets[0]},
        )).fetchone()

        assert row[0] is True
        assert row[1] == ["Crypto"]
        assert row[2] == ["0xmarket_A"]

    async def test_accumulate_categories(self, db_session, clean_wallets):
        from database.manager import DatabaseManager

        db = DatabaseManager(db_session)
        addr = clean_wallets[0]

        await db.upsert_wallet({
            "address": addr, "insider": True,
            "categories_insider": ["Crypto"], "marketids_insider": ["0xA"],
        })
        await db.upsert_wallet({
            "address": addr, "insider": True,
            "categories_insider": ["Politics"], "marketids_insider": ["0xB"],
        })

        row = (await db_session.execute(
            text("SELECT categories_insider, marketids_insider FROM wallets WHERE address = :addr"),
            {"addr": addr},
        )).fetchone()

        assert "Crypto" in row[0]
        assert "Politics" in row[0]
        assert "0xA" in row[1]
        assert "0xB" in row[1]

    async def test_no_duplicate_categories(self, db_session, clean_wallets):
        from database.manager import DatabaseManager

        db = DatabaseManager(db_session)
        addr = clean_wallets[0]

        await db.upsert_wallet({
            "address": addr, "insider": True,
            "categories_insider": ["Crypto"], "marketids_insider": ["0xA"],
        })
        # Same category again
        await db.upsert_wallet({
            "address": addr, "insider": True,
            "categories_insider": ["Crypto"], "marketids_insider": ["0xA"],
        })

        row = (await db_session.execute(
            text("SELECT categories_insider, marketids_insider FROM wallets WHERE address = :addr"),
            {"addr": addr},
        )).fetchone()

        assert row[0].count("Crypto") == 1
        assert row[1].count("0xA") == 1


# ═══════════════════════════════════════════════════════════════════
# 4. T3 task → Redis stream (requires Redis)
# ═══════════════════════════════════════════════════════════════════

class TestPublishInsiderCommand:

    def test_xadd_to_stream(self, redis_client, clean_stream):
        """Run T3 synchronously (eager mode) and verify the Redis stream message."""
        from workers.celery_app import app as celery_app

        celery_app.conf.task_always_eager = True
        celery_app.conf.task_eager_propagates = True
        try:
            from workers.tasks import publish_insider_command

            command = {
                "wallet": "0xaaa",
                "categories_insider": ["Crypto"],
                "market_ids_insider": ["0xmarket1"],
                "command_type": "ADD_WALLET",
            }
            result = publish_insider_command.apply(args=[command]).get()

            assert result["success"] is True
            assert result["wallet"] == "0xaaa"
            assert result["command_type"] == "ADD_WALLET"
            assert result["msg_id"] is not None

            # Verify stream content
            messages = redis_client.xrange("insider_commands")
            assert len(messages) == 1
            payload = json.loads(messages[0][1][b"payload"])
            assert payload["wallet"] == "0xaaa"
            assert payload["categories_insider"] == ["Crypto"]
            assert payload["command_type"] == "ADD_WALLET"
        finally:
            celery_app.conf.task_always_eager = False
            celery_app.conf.task_eager_propagates = False

    def test_multiple_commands_in_stream(self, redis_client, clean_stream):
        from workers.celery_app import app as celery_app

        celery_app.conf.task_always_eager = True
        celery_app.conf.task_eager_propagates = True
        try:
            from workers.tasks import publish_insider_command

            wallets = ["0xaaa", "0xbbb", "0xccc"]
            for w in wallets:
                publish_insider_command.apply(args=[{
                    "wallet": w,
                    "categories_insider": ["Crypto"],
                    "command_type": "ADD_WALLET",
                }]).get()

            messages = redis_client.xrange("insider_commands")
            assert len(messages) == 3

            published_wallets = [
                json.loads(m[1][b"payload"])["wallet"] for m in messages
            ]
            assert set(published_wallets) == set(wallets)
        finally:
            celery_app.conf.task_always_eager = False
            celery_app.conf.task_eager_propagates = False

    def test_payload_matches_contract(self, redis_client, clean_stream):
        """The stream payload must be deserializable as InsiderTradeCommand."""
        from contracts.raw_market_data import InsiderTradeCommand
        from workers.celery_app import app as celery_app

        celery_app.conf.task_always_eager = True
        celery_app.conf.task_eager_propagates = True
        try:
            from workers.tasks import publish_insider_command

            publish_insider_command.apply(args=[{
                "wallet": "0xaaa",
                "categories_insider": ["Crypto", "Politics"],
                "market_ids_insider": ["0xmarket1"],
                "command_type": "UPDATE_CATEGORIES",
            }]).get()

            messages = redis_client.xrange("insider_commands")
            payload_json = messages[0][1][b"payload"]
            cmd = InsiderTradeCommand.model_validate_json(payload_json)

            assert cmd.wallet == "0xaaa"
            assert cmd.categories_insider == ["Crypto", "Politics"]
            assert cmd.market_ids_insider == ["0xmarket1"]
            assert cmd.command_type == "UPDATE_CATEGORIES"
        finally:
            celery_app.conf.task_always_eager = False
            celery_app.conf.task_eager_propagates = False
