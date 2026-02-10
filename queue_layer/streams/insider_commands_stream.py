# Insider Commands Stream — T3 publishes, L2 consumes
# Stream: insider_commands | Consumer group: ingestion_l2

import os
import json
import redis

STREAM_NAME = "insider_commands"
CONSUMER_GROUP = "ingestion_l2"


def get_redis_client() -> redis.Redis:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(redis_url)


def publish_command(client: redis.Redis, command: dict) -> str:
    """
    XADD insider_commands * payload '<json>'

    Args:
        client: Redis client instance
        command: Dict with keys: wallet, categories_insider, market_ids_insider, command_type

    Returns:
        Message ID assigned by Redis
    """
    payload = json.dumps(command)
    msg_id = client.xadd(STREAM_NAME, {"payload": payload})
    return msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)


def ensure_consumer_group(client: redis.Redis) -> None:
    """Create consumer group idempotently (safe to call multiple times)."""
    try:
        client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            pass  # Group already exists
        else:
            raise
