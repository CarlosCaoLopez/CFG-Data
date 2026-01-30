from contracts.raw_market_data import MarketDataBatch, RawTrade
from contracts.processing_job import ProcessingJob, JobType
from datetime import datetime, UTC
from typing import List
import structlog

logger = structlog.get_logger()


class JobCreator:
    """
    Convierte MarketDataBatch en ProcessingJobs individuales.
    Cada trade genera un job para calcular su I_score.
    """
    
    def __init__(self):
        pass
    
    async def process_batch(self, batch: MarketDataBatch) -> List[ProcessingJob]:
        """
        Convierte un batch de mercado en jobs individuales.
        
        Args:
            batch: MarketDataBatch con trades y metadata
            
        Returns:
            Lista de ProcessingJobs listos para encolar
        """
        if not batch.trades:
            logger.warning(
                "batch_empty",
                market_id=batch.metadata.market_id
            )
            return []
        
        jobs = []
        
        for trade in batch.trades:
            job = self._create_job_from_trade(trade, batch)
            jobs.append(job)
        
        logger.info(
            "batch_converted_to_jobs",
            market_id=batch.metadata.market_id,
            trades_count=len(batch.trades),
            jobs_created=len(jobs)
        )
        
        return jobs
    
    def _create_job_from_trade(
        self,
        trade: RawTrade,
        batch: MarketDataBatch
    ) -> ProcessingJob:
        """
        Crea un ProcessingJob individual para un trade.
        """
        job_id = f"{batch.metadata.market_id}:{trade.tx_hash}"

        # Obtener edad de wallet del batch
        wallet_age = batch.wallet_ages.get(trade.wallet)

        payload = {
            'trade': trade.model_dump(),
            'market': batch.metadata.model_dump(),
            'wallet_age': wallet_age.model_dump() if wallet_age else None
        }

        job = ProcessingJob(
            job_id=job_id,
            job_type=JobType.CALCULATE_SCORES,
            created_at=datetime.now(UTC),
            payload=payload
        )

        return job

    async def create_recalculate_wallet_job(
        self,
        wallet: str,
        reason: str = "manual_review"
    ) -> ProcessingJob:
        """
        Crea un job para recalcular todos los scores de una wallet.
        Útil cuando se detecta anomalía o se actualiza el modelo.
        """
        job = ProcessingJob(
            job_id=f"recalc:{wallet}:{datetime.now(UTC).timestamp()}",
            job_type=JobType.RECALCULATE_WALLET,
            created_at=datetime.now(UTC),
            payload={
                'wallet': wallet,
                'reason': reason
            }
        )

        logger.info(
            "recalculate_wallet_job_created",
            wallet=wallet,
            reason=reason
        )

        return job
    
    async def create_update_percentiles_job(
        self,
        market_id: str
    ) -> ProcessingJob:
        """
        Crea un job para recalcular percentiles de un mercado.
        Se ejecuta después de procesar todos los trades de un mercado.
        """
        job = ProcessingJob(
            job_id=f"percentiles:{market_id}:{datetime.now(UTC).timestamp()}",
            job_type=JobType.UPDATE_PERCENTILES,
            created_at=datetime.now(UTC),
            payload={
                'market_id': market_id
            }
        )

        logger.info(
            "update_percentiles_job_created",
            market_id=market_id
        )

        return job 