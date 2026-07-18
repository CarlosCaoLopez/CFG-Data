"""insider_detections table, insiders_trades.is_detection_trade, drop orphaned alerts

Adds a proper detections log (insider_detections) to replace the orphaned
`alerts` table (nobody wrote to it — it belonged to the old continuous
scoring model, not the current binary Motor D criterion). Also adds
is_detection_trade to insiders_trades so the trade(s) that triggered a
detection can be snapshotted before T4 deletes them from `trades`.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-07-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'insider_detections',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('wallet', sa.Text(), nullable=False),
        sa.Column('market_id', sa.Text(), nullable=False),
        sa.Column('categories_insider', sa.ARRAY(sa.Text()), nullable=True),
        sa.Column('detected_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('status', sa.String(length=20), nullable=False, server_default='NEW'),
        sa.Column('reviewed_by', sa.Text(), nullable=True),
        sa.Column('reviewed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['market_id'], ['markets.market_id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('wallet', 'market_id', name='uq_insider_detections_wallet_market'),
    )
    op.create_index('idx_insider_detections_wallet', 'insider_detections', ['wallet'], unique=False)
    op.create_index(
        'idx_insider_detections_detected_at', 'insider_detections',
        [sa.literal_column('detected_at DESC')], unique=False,
    )

    op.add_column(
        'insiders_trades',
        sa.Column('is_detection_trade', sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    op.drop_index('idx_alerts_status', table_name='alerts')
    op.drop_table('alerts')


def downgrade() -> None:
    """Downgrade schema."""
    op.create_table(
        'alerts',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('wallet', sa.Text(), nullable=False),
        sa.Column('market_id', sa.String(length=66), nullable=False),
        sa.Column('i_score', sa.Numeric(), nullable=False),
        sa.Column('percentile', sa.Numeric(), nullable=False),
        sa.Column('alert_type', sa.String(length=50), nullable=False),
        sa.Column('status', sa.String(length=20), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('reviewed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['market_id'], ['markets.market_id'], ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_alerts_status', 'alerts', ['status', sa.literal_column('created_at DESC')], unique=False)

    op.drop_column('insiders_trades', 'is_detection_trade')

    op.drop_index('idx_insider_detections_detected_at', table_name='insider_detections')
    op.drop_index('idx_insider_detections_wallet', table_name='insider_detections')
    op.drop_table('insider_detections')
