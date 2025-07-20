import redis.asyncio as redis
from pydantic import BaseModel
from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from app.utils.retry import RetryHandler

logger = get_logger(__name__)
retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

class RedisSettings(BaseModel):
    url: str = settings.redis.redis_url
    max_connections: int = settings.redis.redis_max_connections
    retry_attempts: int = settings.redis.redis_retry_attempts
    default_ttl: int = 3600
class RedisService:
    def __init__(self, redis_settings: RedisSettings = RedisSettings()):
        self.settings = redis_settings
        self._client: redis.Redis | None = None

    async def __aenter__(self):
        self._client = await self._create_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _create_client(self) -> redis.Redis:
        if self._client:
            return self._client
        try:
            self._client = redis.from_url(
                self.settings.url,
                max_connections=self.settings.max_connections
            )
            await self._client.ping()
            logger.info("Connected to Redis", redis_url=self.settings.url)
            return self._client
        except Exception as e:
            logger.error("Failed to connect to Redis", error=str(e))
            raise

    @retry_handler
    async def get(self, key: str) -> bytes | None:
        client = await self._create_client()
        result = await client.get(key)
        logger.debug("Redis GET", key=key, result=bool(result))
        return result

    @retry_handler
    async def set(self, key: str, value: str, ttl: int | None = None) -> bool:
        client = await self._create_client()
        result = await client.set(key, value)
        if ttl is None:
            ttl = self.settings.default_ttl
        await client.expire(key, ttl)
        logger.debug("Redis SET", key=key, ttl=ttl)
        return result

    @retry_handler
    async def setex(self, key: str, ttl: int, value: str) -> bool:
        client = await self._create_client()
        result = await client.setex(key, ttl, value)
        logger.debug("Redis SETEX", key=key, ttl=ttl)
        return result

    @retry_handler
    async def rpush(self, key: str, value: str) -> int:
        client = await self._create_client()
        result = await client.rpush(key, value)
        logger.debug("Redis RPUSH", key=key, value=value, size=result)
        return result

    @retry_handler
    async def lpop(self, key: str) -> bytes | None:
        client = await self._create_client()
        result = await client.lpop(key)
        logger.debug("Redis LPOP", key=key, result=bool(result))
        return result

    @retry_handler
    async def llen(self, key: str) -> int:
        client = await self._create_client()
        result = await client.llen(key)
        logger.debug("Redis LLEN", key=key, count=result)
        return result

    @retry_handler
    async def exists(self, key: str) -> bool:
        client = await self._create_client()
        result = await client.exists(key)
        return bool(result)

    async def close(self):
        if self._client:
            await self._client.close()
            logger.info("Redis connection closed")
        else:
            logger.warning("Redis client was not initialized, nothing to close")