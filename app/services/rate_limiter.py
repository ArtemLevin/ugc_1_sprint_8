from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

class RedisLeakyBucketRateLimiter:
    def __init__(self, redis_client, rate=10, capacity=20):
        self.redis = redis_client
        self.rate = rate
        self.capacity = capacity

    async def allow_request(self, identifier: str) -> bool:
        key = f"rate_limit:{identifier}"
        now = asyncio.get_event_loop().time()
        window = settings.rate_limit_window
        try:
            lua_script = """
            local key = KEYS[1]
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local window = tonumber(ARGV[4])
            redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
            local current_tokens = redis.call('ZCARD', key)
            if current_tokens < capacity then
                redis.call('ZADD', key, now, now)
                return 1
            else
                return 0
            end
            """
            allowed = await self.redis.eval(lua_script, keys=[key], args=[self.rate, self.capacity, now, window])
            return bool(int(allowed))
        except Exception as e:
            logger.error("Rate limit error", error=str(e))
            return False