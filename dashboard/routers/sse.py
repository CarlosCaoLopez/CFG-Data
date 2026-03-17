"""Server-Sent Events: emite KPIs y estado del pipeline cada 5 segundos."""

import asyncio
import json
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from dashboard.dependencies import _session_factory, _redis
from dashboard.queries.overview_queries import get_kpis
from dashboard.redis_client import get_stream_metrics
from dashboard.config import REDIS_URL

router = APIRouter()

_prev_insider_count: int = 0


async def _event_stream(request: Request):
    global _prev_insider_count
    while not await request.is_disconnected():
        try:
            async with _session_factory() as session:
                kpis = await get_kpis(session)

            redis = _redis
            if redis is None:
                import redis as redis_sync
                from redis.asyncio import Redis as AsyncRedis
                redis = AsyncRedis.from_url(REDIS_URL, decode_responses=True)

            streams = await get_stream_metrics(redis)

            # KPIs event (siempre)
            yield f"event: overview_kpis\ndata: {json.dumps(kpis)}\n\n"

            # Stream lengths event
            lengths = {k: v["length"] for k, v in streams.items()}
            yield f"event: stream_lengths\ndata: {json.dumps(lengths)}\n\n"

            # New detection event (solo si creció el contador)
            current_insiders = kpis.get("total_insiders", 0)
            if current_insiders > _prev_insider_count and _prev_insider_count > 0:
                yield f"event: new_detection\ndata: {json.dumps({'count': current_insiders})}\n\n"
            _prev_insider_count = current_insiders

        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'msg': str(e)})}\n\n"

        await asyncio.sleep(5)


@router.get("/api/sse/live")
async def sse_live(request: Request):
    return StreamingResponse(
        _event_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
