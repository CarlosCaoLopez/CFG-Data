# Alert ORM model
# Table: alerts
from datetime import datetime
from decimal import Decimal
from typing import Optional
from sqlalchemy import String, Numeric, DateTime, ForeignKey, Text, Integer, Index
from sqlalchemy.orm import Mapped, mapped_column
from database.base import Base

class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wallet: Mapped[str] = mapped_column(Text)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"))
    i_score: Mapped[Decimal] = mapped_column(Numeric)
    percentile: Mapped[Decimal] = mapped_column(Numeric)
    alert_type: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(20), default="NEW")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notes: Mapped[Optional[str]] = mapped_column(Text)

    # Relaciones
    market_rel: Mapped["Market"] = relationship(back_populates="scores")

    __table_args__ = (
        Index("idx_alerts_status", "status", created_at.desc()),
    )