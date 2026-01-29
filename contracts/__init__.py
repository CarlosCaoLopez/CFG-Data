# Contracts - Shared interfaces between system layers

from contracts.raw_market_data import (
    RawTrade,
    RawMarketMetadata,
    RawWalletAge,
    MarketDataBatch,
)

from contracts.processing_job import (
    JobType,
    JobPriority,
    ProcessingJob,
    JobResult,
)

from contracts.score_output import (
    WalletScore,
    Alert,
)

__all__ = [
    # Raw data (A → B)
    "RawTrade",
    "RawMarketMetadata",
    "RawWalletAge",
    "MarketDataBatch",

    # Processing jobs (B → C)
    "JobType",
    "JobPriority",
    "ProcessingJob",
    "JobResult",

    # Score output (C → DB)
    "WalletScore",
    "Alert",
]
