# InsidersTrade ORM model
# Table: insiders_trades
# Stores trades detected as insider trading from Redis listener
from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Numeric, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class InsidersTrade(Base):
    __tablename__ = "insiders_trades"

    id: Mapped[str] = mapped_column(String(66), primary_key=True) # Trade ID (hexadecimal)
    wallet: Mapped[str] = mapped_column(String(42), ForeignKey("wallets.address"))
    market_id: Mapped[str] = mapped_column(String(66), ForeignKey("markets.market_id"))
    side: Mapped[str] = mapped_column(String(10))
    outcome: Mapped[str] = mapped_column(String(10))
    prize: Mapped[Decimal] = mapped_column(Numeric)
    size_usdc: Mapped[Decimal] = mapped_column(Numeric)
    size_tokens: Mapped[Decimal] = mapped_column(Numeric)
    tx_hash: Mapped[str] = mapped_column(String(66), unique=True)

    # Relaciones
    wallet_rel: Mapped["Wallet"] = relationship(back_populates="insiders_trades")
    market_rel: Mapped["Market"] = relationship(back_populates="insiders_trades")
