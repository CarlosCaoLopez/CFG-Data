"""Redis stream metrics and Celery worker stats for the dashboard."""

import asyncio
import time
from redis.asyncio import Redis


async def get_stream_metrics(redis: Redis) -> dict:
    streams = {
        "closed_markets": "scheduler",
        "raw_data": "scheduler",
        "insider_trades": "tasks",
        "insider_commands": "ingestion_l2",
    }
    result = {}
    for stream, group in streams.items():
        try:
            length = await redis.xlen(stream)
            try:
                pending_info = await redis.xpending(stream, group)
                pending = pending_info.get("pending", 0) if isinstance(pending_info, dict) else 0
            except Exception:
                pending = 0
            result[stream] = {"length": length, "pending": pending, "group": group}
        except Exception:
            result[stream] = {"length": 0, "pending": 0, "group": group, "error": True}
    return result


# La app Celery se reutiliza entre llamadas: crearla cada vez reabre la conexión
# al broker (caro). El inspect() es un broadcast RPC que SIEMPRE espera el timeout
# completo, así que lo bajamos (los workers locales responden en ms) y cacheamos
# el resultado: el estado de los workers no cambia cada 8s.
_celery_app = None
_CELERY_TTL = 15.0
_celery_cache: dict | None = None
_celery_cache_at: float = 0.0
_celery_lock = asyncio.Lock()


def _celery_inspect_sync(redis_url: str) -> dict:
    global _celery_app
    try:
        if _celery_app is None:
            from celery import Celery
            _celery_app = Celery(broker=redis_url, backend=redis_url)
        i = _celery_app.control.inspect(timeout=0.3)
        active = i.active() or {}
        scheduled = i.scheduled() or {}
        return {
            "workers_online": len(active),
            "active_tasks": sum(len(v) for v in active.values()),
            "scheduled_tasks": sum(len(v) for v in scheduled.values()),
            "worker_names": list(active.keys()),
        }
    except Exception:
        return {"workers_online": 0, "active_tasks": 0, "scheduled_tasks": 0, "worker_names": []}


async def get_celery_stats(redis_url: str) -> dict:
    global _celery_cache, _celery_cache_at

    if _celery_cache is not None and (time.monotonic() - _celery_cache_at) < _CELERY_TTL:
        return _celery_cache

    async with _celery_lock:
        if _celery_cache is not None and (time.monotonic() - _celery_cache_at) < _CELERY_TTL:
            return _celery_cache
        loop = asyncio.get_event_loop()
        _celery_cache = await loop.run_in_executor(None, _celery_inspect_sync, redis_url)
        _celery_cache_at = time.monotonic()
        return _celery_cache
