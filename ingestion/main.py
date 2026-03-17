# Ingestion entry point
#
# Starts both listeners concurrently:
# - L1 (MarketCloseListener): Monitors markets, publishes on close
# - L2 (InsiderTradesListener): Monitors insider wallets from T3 commands
#
# Usage:
#   poetry run python -m ingestion.main

import asyncio
import signal
import structlog
import uvicorn

from ingestion.config import get_settings
from ingestion.publisher import RedisPublisher
from ingestion.listeners.market_close_listener import MarketCloseListener
from ingestion.listeners.insider_trades_listener import InsiderTradesListener
from ingestion.stats_server import stats_app, set_listeners

logger = structlog.get_logger()


async def main() -> None:
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

    l1 = MarketCloseListener(publisher=publisher)
    l2 = InsiderTradesListener(publisher=publisher)

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
        set_listeners(l1, l2)
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
