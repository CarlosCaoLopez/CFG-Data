"""Redis stream metrics and Celery worker stats for the dashboard."""

import asyncio
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


def _celery_inspect_sync(redis_url: str) -> dict:
    try:
        from celery import Celery
        app = Celery(broker=redis_url, backend=redis_url)
        i = app.control.inspect(timeout=0.8)
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
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _celery_inspect_sync, redis_url)
