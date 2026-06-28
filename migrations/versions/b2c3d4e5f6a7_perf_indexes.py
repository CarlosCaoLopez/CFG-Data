"""perf indexes for dashboard queries

Adds the indexes that the dashboard's list/order/join queries rely on but
that the initial schema never created:

- trades.timestamp / insiders_trades.timestamp  -> ORDER BY timestamp DESC
- insiders_trades.market_id                      -> JOIN/filtro por market
- trades.wallet_id                               -> filtro por wallet
- markets.updated_at                             -> ORDER BY m.updated_at DESC

Se crean con CREATE INDEX CONCURRENTLY para no bloquear escrituras en la Pi.
Por eso la migracion corre fuera de transaccion (autocommit).

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-06-28 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_INDEXES = [
    ("idx_trades_timestamp", "trades", "timestamp DESC"),
    ("idx_insiders_trades_timestamp", "insiders_trades", "timestamp DESC"),
    ("idx_insiders_trades_market_id", "insiders_trades", "market_id"),
    ("idx_trades_wallet_id", "trades", "wallet_id"),
    ("idx_markets_updated_at", "markets", "updated_at DESC"),
]


def upgrade() -> None:
    # CONCURRENTLY no puede correr dentro de una transaccion.
    with op.get_context().autocommit_block():
        for name, table, cols in _INDEXES:
            op.execute(
                f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {name} ON {table} ({cols})"
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for name, _table, _cols in _INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
