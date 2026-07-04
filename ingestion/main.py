# Ingestion entry point
#
# Starts both listeners concurrently:
# - L1 (MarketCloseListener): Monitors markets, publishes on close
# - L2 (InsiderTradesListener): Monitors insider wallets from T3 commands
#
# Usage:
#   poetry run python -m ingestion.main

import asyncio
import logging
import os
import signal
import structlog
import uvicorn

from ingestion.config import get_settings
from ingestion.publisher import RedisPublisher
from ingestion.interests_registry import InterestsRegistry
from ingestion.listeners.market_close_listener import MarketCloseListener
from ingestion.listeners.insider_trades_listener import InsiderTradesListener
from ingestion.stats_server import stats_app, set_listeners

logger = structlog.get_logger()


def _configure_logging() -> None:
    """
    Set the structlog level from LOG_LEVEL (default INFO).

    Without this, structlog uses its unfiltered default and the DEBUG
    http_request/http_response logs from http_client flood stdout/journald
    (~330k lines/24h on the Pi, wearing the SD card). INFO drops them.
    Override with LOG_LEVEL=DEBUG for troubleshooting.
    """
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(level),
    )


async def main() -> None:
    _configure_logging()
    settings = get_settings()
    logger.info(
        "ingestion_starting",
        categorias_de_interes=settings.categorias_de_interes,
    )

    publisher = RedisPublisher()

    # Health check Redis before starting listeners
    healthy = await publisher.health_check()
    if not healthy:
        logger.error("redis_health_check_failed_on_startup")
        raise RuntimeError("Cannot connect to Redis. Aborting.")

    logger.info("redis_connection_ok")

    # Single shared category filter for both listeners: one Redis-backed source
    # of truth, so an edit propagates live to L1 and L2 together (and survives
    # redeploys). .env is only the seed for a fresh Redis.
    registry = InterestsRegistry(publisher, settings.categorias_de_interes)
    l1 = MarketCloseListener(publisher=publisher, registry=registry)
    l2 = InsiderTradesListener(publisher=publisher, registry=registry)
    # Load once now that both listeners have subscribed (notifies both).
    await registry.load()

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    async def _shutdown_watcher() -> None:
        await shutdown_event.wait()
        logger.info("stopping_listeners")
        await l1.stop()
        await l2.stop()
        await publisher.close()

    async def _run_stats_server() -> None:
        set_listeners(l1, l2, registry)
        config = uvicorn.Config(
            stats_app,
            host="0.0.0.0",
            port=9001,
            loop="none",
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(config)
        await server.serve()

    try:
        await asyncio.gather(
            l1.run(),
            l2.run(),
            _shutdown_watcher(),
            _run_stats_server(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        await l1.stop()
        await l2.stop()
        await publisher.close()
        logger.info("ingestion_stopped")


if __name__ == "__main__":
    asyncio.run(main())
