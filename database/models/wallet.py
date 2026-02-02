# Wallet ORM model
# Table: wallets
from datetime import datetime, UTC
from typing import List, Optional
from decimal import Decimal
from sqlalchemy import String, Numeric, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class Wallet(Base):
    __tablename__ = "wallets"

    address: Mapped[str] = mapped_column(String(42), primary_key=True)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_tx_polymarket: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    first_tx_polygon: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    total_volume_usdc: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    markets_traded: Mapped[Optional[int]] = mapped_column(Integer)
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(UTC),
        onupdate=datetime.now(UTC)
    )

    # Relaciones
    trades: Mapped[List["Trade"]] = relationship(back_populates="wallet_rel")
    scores: Mapped[List["Score"]] = relationship(back_populates="wallet_rel")