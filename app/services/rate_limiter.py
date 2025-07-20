import asyncio
from pathlib import Path

from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.utils.retry import RetryHandler

logger = get_logger(__name__)

retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

LUA_SCRIPT_PATH = Path(__file__).parent.parent / "utils" / "lua" / "rate_limiting.lua"


class RedisLeakyBucketRateLimiter:
    def __init__(self, redis_pool,  rate=None, capacity=None):
        self.redis_pool = redis_pool
        self.rate = rate or settings.rate_limit.rate_limit
        self.capacity = capacity or settings.rate_limit.rate_limit_window
        self.window = settings.rate_limit.rate_limit_window
        self.script_sha = None
        self.script_loaded = False

    async def load_script(self):
        if not self.script_loaded:
            script_content = self._read_script()
            self.script_sha = await self._load_script_to_redis(script_content)
            self.script_loaded = True

    def _read_script(self):
        with open(LUA_SCRIPT_PATH, 'r') as f:
            return f.read()

    @retry_handler
    async def _load_script_to_redis(self, script):
        client = await self.redis_pool.get_client()
        sha = await client.script_load(script)
        return sha

    async def allow_request(self, identifier: str) -> bool:
        await self.load_script()

        key = f"rate_limit:{identifier}"
        now = asyncio.get_event_loop().time()
        now_ms = now * 1000
        window_ms = self.window * 1000

        try:
            allowed = await self._execute_script(key, now_ms, window_ms)
            return bool(int(allowed))
        except Exception as e:
            logger.error("Rate limit error", error=str(e))
            return False

    async def _execute_script(self, key, now_ms, window_ms):
        client = await self.redis_pool.get_client()
        result = await client.evalsha(
            self.script_sha,
            keys=[key],
            args=[self.rate, self.capacity, now_ms, window_ms]
        )
        return result