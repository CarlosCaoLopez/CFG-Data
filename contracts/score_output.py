# C → DB: WalletScore and Alert contracts
# Defines output structures for calculated scores and generated alerts

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Literal


class WalletScore(BaseModel):
    """Complete I_score calculation for a single trade"""
    # Identification
    wallet: str = Field(..., description="Wallet address")
    market_id: str = Field(..., description="Market condition ID")
    trade_tx_hash: str = Field(..., description="Trade transaction hash")

    # Categorization (for analysis grouping and filtering)
    category: str | None = Field(
        None,
        description="Market category (e.g., 'Sports', 'US-current-affairs', 'Crypto')"
    )
    tags: list[str] | None = Field(
        None,
        description="Market tags for granular filtering (e.g., ['NBA', 'Basketball'])"
    )

    # Final scores
    i_score: float = Field(..., description="Combined insider score")
    percentile: float | None = Field(
        None,
        ge=0.0,
        le=1.0,
        description="Percentile within market (0.0-1.0)"
    )

    # Components
    component_a: float = Field(..., description="Impacto del Éxito: ΔELO·ln(1+V)")
    component_b: float = Field(..., description="Factor Fantasma: 1/√(Age·Δt_res)")
    component_c: float = Field(..., description="Agresividad Focalizada: α·e^(-H)")

    # Intermediate variables for transparency
    delta_elo: float = Field(..., description="ELO change: K×(S-P_mkt)")
    volume_usdc: float = Field(..., ge=0, description="Trade volume in USDC")
    won: bool = Field(..., description="Whether the trade won")
    price_at_trade: float = Field(..., ge=0.0, le=1.0, description="Market price at trade")
    age_days: float = Field(..., ge=0, description="Wallet age in days")
    hours_to_resolution: float = Field(..., ge=0, description="Hours until market close")
    slippage: float = Field(..., ge=0, description="Price impact (α_slippage)")
    portfolio_entropy: float = Field(..., ge=0, description="Portfolio concentration (H)")

    # Metadata
    calculated_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When score was calculated"
    )
    parameters_version: str = Field(
        default="v1.0",
        description="Version of parameters used (θ₁, θ₂, θ₃, K)"
    )


class Alert(BaseModel):
    """Alert generated for suspicious wallet activity"""
    wallet: str = Field(..., description="Wallet address")
    market_id: str = Field(..., description="Market condition ID")

    # Categorization (useful for filtering alerts by domain)
    category: str | None = Field(
        None,
        description="Market category (e.g., 'Sports', 'US-current-affairs', 'Crypto')"
    )

    # Score details
    i_score: float = Field(..., description="Insider score that triggered alert")
    percentile: float = Field(..., ge=0.0, le=1.0, description="Score percentile")

    # Alert classification
    alert_type: Literal["P95", "P99", "EXTREME"] = Field(
        ...,
        description="Alert severity level"
    )

    # Metadata
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When alert was created"
    )
