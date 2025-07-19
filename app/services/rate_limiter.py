from app.core.config import settings
from app.core.logger import get_logger
import asyncio

logger = get_logger(__name__)


class RedisLeakyBucketRateLimiter:
    def __init__(self, redis_pool, rate=None, capacity=None):
        self.redis_pool = redis_pool
        self.rate = rate or settings.rate_limit.rate_limit
        self.capacity = capacity or settings.rate_limit.rate_limit_window
        self.window = settings.rate_limit.rate_limit_window

    async def allow_request(self, identifier: str) -> bool:
        key = f"rate_limit:{identifier}"
        now = asyncio.get_event_loop().time()

        now_ms = now * 1000
        window_ms = self.window * 1000

        lua_script = """
        local key = KEYS[1]
        local rate = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local window = tonumber(ARGV[4])

        -- Удаляем устаревшие записи (в миллисекундах)
        redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

        local current_tokens = redis.call('ZCARD', key)
        if current_tokens < capacity then
            redis.call('ZADD', key, now, now)
            return 1
        else
            return 0
        end
        """

        try:
            allowed = await self.redis_pool.get_client().eval(
                lua_script,
                keys=[key],
                args=[self.rate, self.capacity, now_ms, window_ms]
            )
            return bool(int(allowed))
        except Exception as e:
            logger.error("Rate limit error", error=str(e))
            return False