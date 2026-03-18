import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from config.settings import settings
from db._utils import redact_url

logger = logging.getLogger(__name__)


async def connect_redis() -> Redis:
    """Connect to Redis and verify the connection with a ping."""
    client: Redis = Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await client.ping()  # type: ignore[misc]
    except RedisError as e:
        await client.aclose()
        raise RuntimeError(
            f"Could not connect to Redis at {redact_url(settings.redis_url)}: {e}"
        ) from e
    return client
