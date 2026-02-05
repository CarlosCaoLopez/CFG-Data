from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from database.models.market import Market
from database.models.wallet import Wallet
from database.models.trade import Trade
from database.models.insiders_trade import InsidersTrade
from database.models.closed_market_variables import ClosedMarketVariables

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
                "volume_usdc": stmt.excluded.volume_usdc,
                "categories": stmt.excluded.categories,
                "tags": stmt.excluded.tags,
                "winning_outcome": stmt.excluded.winning_outcome
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
                "markets_traded": stmt.excluded.markets_traded,
                "insider": stmt.excluded.insider,
                "categories_insider": stmt.excluded.categories_insider,
                "marketids_insider": stmt.excluded.marketids_insider
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


    async def add_insider_trade(self, insider_trade_data: dict):
        """
        Inserts a new insider trade
        """
        stmt = insert(InsidersTrade).values(**insider_trade_data)

        await self.session.execute(stmt)
        await self.session.commit()


    async def upsert_closed_market_variables(self, cmv_data: dict):
        """
        Inserts or updates closed market variables (composite key: wallet + market_id)
        """
        stmt = insert(ClosedMarketVariables).values(**cmv_data)

        # What to do if the composite primary key exists
        stmt = stmt.on_conflict_do_update(
            index_elements=['wallet', 'market_id'],
            set_={
                "delta_t": stmt.excluded.delta_t,
                "age": stmt.excluded.age,
                "winning_volume": stmt.excluded.winning_volume,
                "alpha_slippage": stmt.excluded.alpha_slippage,
                "entropy": stmt.excluded.entropy
            }
        )

        await self.session.execute(stmt)
        await self.session.commit()