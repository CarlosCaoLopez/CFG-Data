# ClosedMarketVariables ORM model
# Table: closed_market_variables
# Temporary table calculated by Task 2 at market close via SQL script
# Stores intermediate variables for score calculation
from decimal import Decimal
from sqlalchemy import String, Numeric, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class ClosedMarketVariables(Base):
    __tablename__ = "closed_market_variables"

    # Composite primary key
    wallet: Mapped[str] = mapped_column(String(42), ForeignKey("wallets.address"), primary_key=True)
    market_id: Mapped[str] = mapped_column(String(66), ForeignKey("markets.market_id"), primary_key=True)

    # Calculated variables for insider detection
    delta_t: Mapped[Decimal] = mapped_column(Numeric) # Time difference in hours
    age: Mapped[Decimal] = mapped_column(Numeric) # Account age in days
    winning_volume: Mapped[Decimal] = mapped_column(Numeric) # Volume on winning outcome
    alpha_slippage: Mapped[Decimal] = mapped_column(Numeric) # α - trade slippage
    entropy: Mapped[Decimal] = mapped_column(Numeric) # H - market entropy

    # Relaciones
    wallet_rel: Mapped["Wallet"] = relationship(back_populates="closed_market_vars")
    market_rel: Mapped["Market"] = relationship(back_populates="closed_market_vars")
