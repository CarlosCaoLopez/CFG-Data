from sqlalchemy import text as sa_text
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
        Inserts or updates a wallet, avoiding duplicates.
        Arrays categories_insider and marketids_insider are accumulated (append + dedup),
        not replaced, so previous detections are preserved.
        """
        stmt = insert(Wallet).values(**wallet_data)

        stmt = stmt.on_conflict_do_update(
            index_elements=['address'],
            set_={
                "total_volume_usdc": stmt.excluded.total_volume_usdc,
                "markets_traded": stmt.excluded.markets_traded,
                "insider": stmt.excluded.insider,
                "categories_insider": sa_text(
                    "ARRAY(SELECT DISTINCT unnest("
                    "COALESCE(wallets.categories_insider, '{}'::text[]) || "
                    "COALESCE(EXCLUDED.categories_insider, '{}'::text[])))"
                ),
                "marketids_insider": sa_text(
                    "ARRAY(SELECT DISTINCT unnest("
                    "COALESCE(wallets.marketids_insider, '{}'::text[]) || "
                    "COALESCE(EXCLUDED.marketids_insider, '{}'::text[])))"
                ),
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


    async def add_market_batch(self, markets: list[dict]):
        """Inserts a batch of markets in a single statement."""
        stmt = insert(Market).values(markets)
        await self.session.execute(stmt)
        await self.session.commit()

    async def add_wallet_batch(self, wallets: list[dict]):
        """Inserts a batch of wallets in a single statement."""
        stmt = insert(Wallet).values(wallets)
        await self.session.execute(stmt)
        await self.session.commit()

    async def add_trade_batch(self, trades: list[dict]):
        """Inserts a batch of trades in a single statement."""
        stmt = insert(Trade).values(trades)
        await self.session.execute(stmt)
        await self.session.commit()

    async def add_insider_trade_batch(self, insider_trades: list[dict]):
        """Inserts a batch of insider trades in a single statement."""
        stmt = insert(InsidersTrade).values(insider_trades)
        await self.session.execute(stmt)
        await self.session.commit()

    async def upsert_trade_batch(self, trades: list[dict]):
        """
        Inserts a batch of trades, ignoring duplicates (idempotent).
        Trades are immutable once written, so ON CONFLICT DO NOTHING is correct.
        """
        stmt = insert(Trade).values(trades)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        await self.session.execute(stmt)
        await self.session.commit()

    async def upsert_insider_trade_batch(self, insider_trades: list[dict]):
        """
        Inserts a batch of insider trades, ignoring duplicates (idempotent).
        """
        stmt = insert(InsidersTrade).values(insider_trades)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        await self.session.execute(stmt)
        await self.session.commit()

    async def upsert_closed_market_variables(self, cmv_data: dict):
        """
        This function should not be used now
        Inserts or updates closed market variables (composite key: wallet_id + market_id + trade_id)
        """
        stmt = insert(ClosedMarketVariables).values(**cmv_data)

        # What to do if the composite primary key exists
        stmt = stmt.on_conflict_do_update(
            index_elements=['wallet_id', 'market_id', 'trade_id'],
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