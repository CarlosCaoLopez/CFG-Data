# InsidersTrade ORM model
# Table: insiders_trades
# Stores trades detected as insider trading from Redis listener
from datetime import datetime, UTC
from decimal import Decimal
from sqlalchemy import String, Numeric, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class InsidersTrade(Base):
    __tablename__ = "insiders_trades"
    __table_args__ = (
        Index("idx_insiders_trades_wallet_id", "wallet_id"),
    )

    id: Mapped[str] = mapped_column(String(66), primary_key=True) # Trade ID (hexadecimal)
    wallet_id: Mapped[str] = mapped_column(String(42), ForeignKey("wallets.address"))
    market_id: Mapped[str] = mapped_column(String(66), ForeignKey("markets.market_id"))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    side: Mapped[str] = mapped_column(String(10))
    outcome: Mapped[str] = mapped_column(String(10))
    prize: Mapped[Decimal] = mapped_column(Numeric)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric)
    size_tokens: Mapped[Decimal] = mapped_column(Numeric)
    last_trade: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(UTC),
        onupdate=datetime.now(UTC)
    )

    # Relaciones
    wallet_rel: Mapped["Wallet"] = relationship(back_populates="insiders_trades")
    market_rel: Mapped["Market"] = relationship(back_populates="insiders_trades")
