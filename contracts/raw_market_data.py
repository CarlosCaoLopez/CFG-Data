# A → B: MarketDataBatch contract
# Defines data structures for raw market data ingestion

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal


class RawTrade(BaseModel):
    """Represents a single trade from Polymarket"""
    tx_hash: str = Field(..., description="Transaction hash on Polygon")
    wallet: str = Field(..., description="Wallet address (lowercase, 0x-prefixed)")
    market_id: str = Field(..., description="conditionId from Polymarket")
    timestamp: datetime = Field(..., description="Trade execution timestamp")
    side: Literal["BUY", "SELL"] = Field(..., description="Trade direction")
    outcome: Literal["Yes", "No"] = Field(..., description="Outcome traded")
    price: float = Field(..., ge=0.0, le=1.0, description="Price (0.0 - 1.0)")
    size_usdc: float = Field(..., ge=0, description="Trade size in USDC")
    size_tokens: float = Field(..., ge=0, description="Trade size in tokens")


class RawMarketMetadata(BaseModel):
    """Metadata about a Polymarket market"""
    market_id: str = Field(..., description="conditionId from Polymarket")
    question: str = Field(..., description="Market question")
    end_date: datetime = Field(..., description="Market end/resolution date")
    closed: bool = Field(..., description="Whether market is closed")
    outcome_prices: list[float] | None = Field(
        None,
        description="Final outcome prices if resolved (e.g., [1.0, 0.0])"
    )
    volume_usdc: float = Field(..., ge=0, description="Total market volume in USDC")

    # Categorization fields
    category: str | None = Field(
        None,
        description="Market category (e.g., 'Sports', 'US-current-affairs', 'Crypto')"
    )
    tags: list[str] | None = Field(
        None,
        description="List of tag labels from associated events (e.g., ['NBA', 'Basketball'])"
    )


class RawWalletAge(BaseModel):
    """Wallet age information from multiple sources"""
    wallet: str = Field(..., description="Wallet address (lowercase, 0x-prefixed)")
    first_tx_polymarket: datetime | None = Field(
        None,
        description="First transaction on Polymarket"
    )
    first_tx_polygon: datetime | None = Field(
        None,
        description="First transaction on Polygon (from Polygonscan)"
    )


class MarketDataBatch(BaseModel):
    """Complete batch of market data for processing"""
    metadata: RawMarketMetadata = Field(..., description="Market metadata")
    trades: list[RawTrade] = Field(..., description="All trades in this market")
    wallet_ages: dict[str, RawWalletAge] = Field(
        ...,
        description="Wallet age data keyed by wallet address"
    )
    ingested_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When this batch was ingested"
    )
