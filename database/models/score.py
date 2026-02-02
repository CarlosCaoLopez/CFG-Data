# Score ORM model
# Table: scores
from datetime import datetime, UTC
from decimal import Decimal
from typing import Optional
from sqlalchemy import Numeric, DateTime, ForeignKey, Text, Integer, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class Score(Base):
    __tablename__ = "scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wallet: Mapped[str] = mapped_column(ForeignKey("wallets.address"))
    trade_id: Mapped[int] = mapped_column(ForeignKey("trades.id"))
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"))
    i_score: Mapped[Decimal] = mapped_column(Numeric)
    component_a: Mapped[Decimal] = mapped_column(Numeric)
    component_b: Mapped[Decimal] = mapped_column(Numeric)
    component_c: Mapped[Decimal] = mapped_column(Numeric)
    component_d: Mapped[Decimal] = mapped_column(Numeric)
    percentile: Mapped[Decimal] = mapped_column(Numeric)
    calculated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.now(UTC))

    # Debugging
    delta_elo: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    volume: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    age_days: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    delta_t_hours: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    slippage: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    entropy: Mapped[Optional[Decimal]] = mapped_column(Numeric)

    # Relaciones
    wallet_rel: Mapped["Wallet"] = relationship(back_populates="scores")
    trade_rel: Mapped["Trade"] = relationship(back_populates="scores")
    market_rel: Mapped["Market"] = relationship(back_populates="scores")

    __table_args__ = (
        Index("idx_scores_market", "market_id", i_score.desc()),
        Index("idx_scores_percentile", percentile.desc()),
    )