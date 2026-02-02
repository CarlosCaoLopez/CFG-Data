# Market ORM model
# Table: markets
from datetime import datetime, UTC
from typing import List, Optional
from decimal import Decimal
from sqlalchemy import Text, Boolean, DateTime, Numeric, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class Market(Base):
    __tablename__ = "markets"

    market_id: Mapped[str] = mapped_column(Text, primary_key=True) # conditionId
    question: Mapped[str] = mapped_column(Text, nullable=False)
    end_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    closed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Usamos ARRAY de Postgres para los precios de los outcomes
    outcome_prices: Mapped[Optional[List[float]]] = mapped_column(ARRAY(Numeric))

    volume_usdc: Mapped[Decimal] = mapped_column(Numeric, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(UTC),
        onupdate=datetime.now(UTC)
    )

    # Relaciones para navegar desde el mercado a sus datos
    trades: Mapped[List["Trade"]] = relationship(back_populates="market_rel")
    scores: Mapped[List["Score"]] = relationship(back_populates="market_rel")
    alerts: Mapped[List["Alert"]] = relationship(back_populates="market_rel")