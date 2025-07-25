from app_v_1.utils.cache import RedisService
from app_v_1.core.logger import get_logger
from app_v_1.repositories.repository import CacheRepository
from typing import TypeVar, Generic
from pydantic import BaseModel
import structlog
from opentelemetry import trace

logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)

T = TypeVar("T", bound=BaseModel)

class RedisRepository(CacheRepository[T], Generic[T]):
    def __init__(self, redis_service: RedisService, model: type[T]):
        self.redis = redis_service
        self.model = model

    async def _log_and_trace(self, key: str, method: str):
        with tracer.start_as_current_span(f"redis.{method}") as span:
            span.set_attribute("db.system", "redis")
            span.set_attribute("db.operation", method)
            span.set_attribute("db.redis.key", key)
            return span

    async def get(self, key: str) -> T | None:
        span = await self._log_and_trace(key, "get")
        result = await self.redis.get(key)
        span.set_attribute("db.redis.exists", bool(result))
        logger.info("Redis GET", key=key, exists=bool(result))
        return self.model.model_validate_json(result) if result else None

    async def set(self, key: str, value: T, ttl: int | None = None) -> bool:
        span = await self._log_and_trace(key, "set")
        result = await self.redis.set(key, value.model_dump_json(), ttl=ttl)
        span.set_attribute("db.redis.result", result)
        logger.info("Redis SET", key=key, result=result, ttl=ttl)
        return result

    async def setex(self, key: str, ttl: int, value: T) -> bool:
        span = await self._log_and_trace(key, "setex")
        result = await self.redis.setex(key, ttl, value.model_dump_json())
        span.set_attribute("db.redis.result", result)
        logger.info("Redis SETEX", key=key, result=result, ttl=ttl)
        return result

    async def rpush(self, key: str, value: T) -> int:
        span = await self._log_and_trace(key, "rpush")
        result = await self.redis.rpush(key, value.model_dump_json())
        span.set_attribute("db.redis.length", result)
        logger.info("Redis RPUSH", key=key, length=result)
        return result

    async def lpop(self, key: str) -> T | None:
        span = await self._log_and_trace(key, "lpop")
        result = await self.redis.lpop(key)
        span.set_attribute("db.redis.exists", bool(result))
        logger.info("Redis LPOP", key=key, exists=bool(result))
        return self.model.model_validate_json(result) if result else None

    async def llen(self, key: str) -> int:
        span = await self._log_and_trace(key, "llen")
        result = await self.redis.llen(key)
        span.set_attribute("db.redis.length", result)
        logger.info("Redis LLEN", key=key, length=result)
        return result

    async def exists(self, key: str) -> bool:
        span = await self._log_and_trace(key, "exists")
        result = await self.redis.exists(key)
        span.set_attribute("db.redis.exists", result)
        logger.info("Redis EXISTS", key=key, exists=result)
        return result