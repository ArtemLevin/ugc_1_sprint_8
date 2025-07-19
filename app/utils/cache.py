import redis.asyncio as redis

from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.retry import RetryHandler

logger = get_logger(__name__)

retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

def redis_command(func):
    async def wrapper(self, *args, **kwargs):
        client = await self.get_client()
        return await func(self, client, *args, **kwargs)
    return wrapper

class RedisService:
    def __init__(self, url=None, max_connections=10, max_retries=3):
        self.url = url or settings.redis.redis_url
        self.max_connections = max_connections or settings.redis.redis_max_connections
        self.max_retries = max_retries or settings.redis.redis_retry_attempts
        self._client = None

    @retry_handler
    async def get_client(self):
        if not self._client:
            try:
                self._client = redis.from_url(self.url, max_connections=self.max_connections)
                await self._client.ping()
            except Exception as e:
                logger.error("Failed to connect to Redis", error=str(e))
                raise
        return self._client

    @redis_command
    async def get(self, client, key):
        return await client.get(key)

    @redis_command
    async def set(self, client, key, value):
        return await client.set(key, value)

    @redis_command
    async def setex(self, client, key, ttl, value):
        return await client.setex(key, ttl, value)

    @redis_command
    async def rpush(self, client, key, value):
        return await client.rpush(key, value)

    @redis_command
    async def lpop(self, client, key):
        return await client.lpop(key)

    @redis_command
    async def llen(self, client, key):
        return await client.llen(key)

    async def close(self):
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
        else:
            logger.warning("Redis client was not initialized, nothing to close")