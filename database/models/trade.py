# Trade ORM model
# Table: trades
from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Numeric, DateTime, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    wallet: Mapped[str] = mapped_column(ForeignKey("wallets.address"))
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    side: Mapped[str] = mapped_column(String(10))
    outcome: Mapped[str] = mapped_column(String(10))
    price: Mapped[Decimal] = mapped_column(Numeric)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric)
    size_tokens: Mapped[Decimal] = mapped_column(Numeric)
    tx_hash: Mapped[str] = mapped_column(String(66), unique=True)

    # Relaciones
    wallet_rel: Mapped["Wallet"] = relationship(back_populates="trades")
    market_rel: Mapped["Market"] = relationship(back_populates="trades")
    scores: Mapped[list["Score"]] = relationship(back_populates="trade_rel")