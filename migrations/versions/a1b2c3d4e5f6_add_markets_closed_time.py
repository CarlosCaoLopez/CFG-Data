"""add markets.closed_time

Adds the actual resolution time (closedTime from Gamma) to markets.
end_date is the scheduled/nominal end and is often in the future even after
a market resolves; closed_time is the real close date.

Revision ID: a1b2c3d4e5f6
Revises: 51547900f901
Create Date: 2026-06-24 17:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '51547900f901'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'markets',
        sa.Column('closed_time', sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('markets', 'closed_time')
