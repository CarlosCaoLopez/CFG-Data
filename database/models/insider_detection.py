# InsiderDetection ORM model
# Table: insider_detections
# Log of insider detections (E writes one row per wallet+market when Motor D
# flags a wallet). Replaces the orphaned `alerts` table from the old
# continuous scoring model.
from datetime import datetime, UTC
from typing import List, Optional
from sqlalchemy import String, Text, DateTime, ForeignKey, Index, UniqueConstraint, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database.base import Base

class InsiderDetection(Base):
    __tablename__ = "insider_detections"
    __table_args__ = (
        UniqueConstraint("wallet", "market_id", name="uq_insider_detections_wallet_market"),
        Index("idx_insider_detections_wallet", "wallet"),
        Index("idx_insider_detections_detected_at", "detected_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    wallet: Mapped[str] = mapped_column(Text)
    market_id: Mapped[str] = mapped_column(ForeignKey("markets.market_id"))
    categories_insider: Mapped[Optional[List[str]]] = mapped_column(ARRAY(Text))
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    status: Mapped[str] = mapped_column(String(20), default="NEW")
    reviewed_by: Mapped[Optional[str]] = mapped_column(Text)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    notes: Mapped[Optional[str]] = mapped_column(Text)

    # Relaciones
    market_rel: Mapped["Market"] = relationship(back_populates="insider_detections")
