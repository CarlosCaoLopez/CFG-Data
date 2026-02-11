"""
Entry point for the QueueOrchestrator.

Usage:
    python -m queue_system          # run orchestrator
    docker-compose up orchestrator  # run via Docker
"""
import asyncio
from redis.asyncio import Redis
from queue_system.orchestrator import QueueOrchestrator
from queue_system.streams.raw_data_stream import RawDataStream
from queue_system.streams.jobs_stream import JobsStream
from queue_system.streams.insider_trades_stream import InsiderTradesStream
from queue_system.scheduler.job_creator import JobCreator
from queue_system.config import QueueConfig
import structlog

logger = structlog.get_logger()


async def main():
    config = QueueConfig()
    redis = Redis.from_url(config.redis_url)

    raw_stream = RawDataStream(redis, config.raw_data_stream_name)
    closed_stream = RawDataStream(redis, config.closed_markets_stream_name)
    jobs_stream = JobsStream(redis, config.jobs_stream_name)
    insider_stream = InsiderTradesStream(redis, config.insider_trades_stream_name)
    creator = JobCreator()

    logger.info(
        "orchestrator_initializing",
        redis_url=config.redis_url,
        raw_data_stream=config.raw_data_stream_name,
        closed_markets_stream=config.closed_markets_stream_name,
        insider_trades_stream=config.insider_trades_stream_name,
    )

    async with QueueOrchestrator(
        raw_data_stream=raw_stream,
        jobs_stream=jobs_stream,
        job_creator=creator,
        closed_markets_stream=closed_stream,
        insider_trades_stream=insider_stream,
        config=config,
    ) as orch:
        await orch.run()


if __name__ == "__main__":
    asyncio.run(main())
