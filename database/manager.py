from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from database.models.market import Market
from database.models.wallet import Wallet
from database.models.trade  import Trade
from database.models.score  import Score

class DatabaseManager:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_market(self, market_data: dict):
        """
        Inserts or updates a market, avoiding duplicates
        """
        stmt = insert(Market).values(**market_data)

        # What to do if the primary key exists
        stmt = stmt.on_conflict_do_update(
            index_elements=['market_id'],
            set_={
                "question": stmt.excluded.question,
                "end_date": stmt.excluded.end_date,
                "closed": stmt.excluded.closed,
                "outcome_prices": stmt.excluded.outcome_prices,
                "volume_usdc": stmt.excluded.volume_usdc
            }
        )

        await self.session.execute(stmt)
        await self.session.commit()


    async def upsert_wallet(self, wallet_data: dict):
        """
        Inserts or updates a wallet, avoiding duplicates
        """
        stmt = insert(Wallet).values(**wallet_data)

        # What to do if the primary key exists
        stmt = stmt.on_conflict_do_update(
            index_elements=['address'],
            set_={
                "total_volume_usdc": stmt.excluded.total_volume_usdc,
                "markets_traded": stmt.excluded.markets_traded
            }
        )

        await self.session.execute(stmt)
        await self.session.commit()


    async def add_trade(self, trade_data: dict):
        """
        Inserts a new trade
        """
        stmt = insert(Trade).values(**trade_data)

        await self.session.execute(stmt)
        await self.session.commit()


    async def add_score(self, score_data: dict):
        """
        Inserts a new score
        """
        stmt = insert(Score).values(**score_data)

        await self.session.execute(stmt)
        await self.session.commit()