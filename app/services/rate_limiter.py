import asyncio
from abc import ABC, abstractmethod
from pathlib import Path
from app.core.config import settings
from app.core.logger import get_logger
from tenacity import retry, stop_after_attempt, wait_exponential
from app.utils.retry import RetryHandler
from app.utils.cache import RedisService

logger = get_logger(__name__)
retry_handler = RetryHandler(max_retries=3, base_delay=1, max_delay=10)

LUA_SCRIPT_PATH = Path(__file__).parent.parent / "utils" / "lua" / "rate_limiting.lua"

class RateLimiter(ABC):
    @abstractmethod
    async def allow_request(self, identifier: str) -> bool:
        pass

class RedisLeakyBucketRateLimiter(RateLimiter):
    def __init__(
        self,
        redis_service: RedisService,
        rate: int | None = None,
        capacity: int | None = None,
        window: int | None = None
    ):
        self.redis_service = redis_service
        self.rate = rate or settings.rate_limit.rate_limit
        self.capacity = capacity or settings.rate_limit.rate_limit_window
        self.window = window or settings.rate_limit.rate_limit_window
        self.script_sha = None
        self.script_loaded = False
        self.lua_script = self._read_script()

    def _read_script(self) -> str:
        try:
            with open(LUA_SCRIPT_PATH, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error("Failed to read Lua script", error=str(e))
            raise

    @retry_handler
    async def _load_script_to_redis(self) -> str:
        client = await self.redis_service.get_client()
        sha = await client.script_load(self.lua_script)
        return sha

    async def load_script(self):
        if not self.script_loaded:
            self.script_sha = await self._load_script_to_redis()
            self.script_loaded = True

    async def allow_request(self, identifier: str) -> bool:
        await self.load_script()
        key = f"rate_limit:{identifier}"
        now = asyncio.get_event_loop().time() * 1000  # ms
        window_ms = self.window * 1000  # ms
        try:
            result = await self._execute_script(key, now, window_ms)
            return bool(int(result))
        except Exception as e:
            logger.warning("Rate limit check failed", error=str(e), identifier=identifier)
            return await self._fallback_rate_limit(identifier)

    async def _execute_script(self, key: str, now: float, window: float) -> int:
        client = await self.redis_service.get_client()
        result = await client.evalsha(
            self.script_sha,
            keys=[key],
            args=[self.rate, self.capacity, now, window]
        )
        return int(result)

    async def _fallback_rate_limit(self, identifier: str) -> bool:
        """Fallback: если Redis недоступен, разрешаем запрос с логированием"""
        logger.warning("Fallback rate limit", identifier=identifier)
        return True