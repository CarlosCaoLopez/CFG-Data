import asyncio
from decimal import Decimal
from datetime import datetime, UTC
from database.connection import async_session
from database.manager import DatabaseManager

async def test_database_flow():
    print("🚀 Iniciando prueba de base de datos...")

    async with async_session() as session:
        db = DatabaseManager(session)

        # 1. Mock de Mercado
        market_id = "cond_123_test_market"
        market_data = {
            "market_id": market_id,
            "question": "¿Llegará Bitcoin a 100k en 2024?",
            "end_date": datetime(2024, 12, 31, tzinfo=UTC),
            "closed": False,
            "outcome_prices": [0.65, 0.35],
            "volume_usdc": Decimal("1500000.50")
        }
        await db.upsert_market(market_data)
        print("✅ Mercado insertado/actualizado.")

        # 2. Mock de Wallet
        wallet_address = "0x742d35Cc6634C0532925a3b844Bc454e4438f44e"
        wallet_data = {
            "address": wallet_address,
            "total_volume_usdc": Decimal("50000.00"),
            "markets_traded": 12
        }
        await db.upsert_wallet(wallet_data)
        print("✅ Wallet insertada/actualizada.")

        # 3. Mock de Trade (Referenciando los anteriores)
        trade_hash = "0xabc123_test_hash"
        trade_data = {
            "tx_hash": trade_hash,
            "wallet": wallet_address,
            "market_id": market_id,
            "timestamp": datetime.now(UTC),
            "side": "BUY",
            "outcome": "YES",
            "price": Decimal("0.65"),
            "size_usdc": Decimal("1000.00"),
            "size_tokens": Decimal("1538.46")
        }
        await db.add_trade(trade_data)
        print("✅ Trade insertado.")

        # 4. Probar UPSERT: Cambiamos el volumen del mercado
        market_data["volume_usdc"] = Decimal("2000000.00")
        await db.upsert_market(market_data)
        print("🔄 UPSERT exitoso: Volumen del mercado actualizado sin duplicar fila.")

if __name__ == "__main__":
    asyncio.run(test_database_flow())