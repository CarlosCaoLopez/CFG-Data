# B → C: ProcessingJob contract
# Defines job types and processing workflow structures

from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class JobType(str, Enum):
    """Type of processing job"""
    CALCULATE_SCORES = "calculate_scores"
    RECALCULATE_WALLET = "recalculate_wallet"
    UPDATE_PERCENTILES = "update_percentiles"


class JobPriority(int, Enum):
    """Job priority levels"""
    LOW = 1         # Backfill histórico
    NORMAL = 5      # Mercados recién cerrados
    HIGH = 10       # Tiempo real


class ProcessingJob(BaseModel):
    """Job definition for worker processing"""
    job_id: str = Field(..., description="Unique job identifier")
    job_type: JobType = Field(..., description="Type of processing to perform")
    priority: JobPriority = Field(..., description="Job priority level")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="When the job was created"
    )
    payload: dict = Field(
        ...,
        description="Job-specific data (structure depends on job_type)"
    )


class JobResult(BaseModel):
    """Result of a completed processing job"""
    job_id: str = Field(..., description="ID of the completed job")
    success: bool = Field(..., description="Whether job completed successfully")
    error_message: str | None = Field(
        None,
        description="Error message if job failed"
    )
    scores_calculated: int = Field(
        0,
        description="Number of I_scores calculated"
    )
    alerts_generated: int = Field(
        0,
        description="Number of alerts generated"
    )
    processing_time_ms: int = Field(
        ...,
        description="Processing time in milliseconds"
    )
