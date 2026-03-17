from fastapi import APIRouter, Depends
from redis.asyncio import Redis
from dashboard.dependencies import get_redis
from dashboard.redis_client import get_stream_metrics, get_celery_stats
from dashboard.l1_stats_client import get_l1_stats
from dashboard.config import REDIS_URL

router = APIRouter()


@router.get("/api/pipeline")
async def pipeline(redis: Redis = Depends(get_redis)):
    streams, celery, l1 = await __import__("asyncio").gather(
        get_stream_metrics(redis),
        get_celery_stats(REDIS_URL),
        get_l1_stats(),
    )
    return {"redis": streams, "celery": celery, "l1_stats": l1}
