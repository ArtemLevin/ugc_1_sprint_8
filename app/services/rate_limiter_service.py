from app.core.logger import get_logger
from typing import Any

logger = get_logger(__name__)


class RateLimiterService:
    def __init__(self, rate_limiter: "RedisLeakyBucketRateLimiter"):
        self.rate_limiter = rate_limiter

    async def check(self, event: Any) -> bool:
        """
        Проверяет, не превышен ли рейт-лимит для пользователя и типа события.

        :param event: объект события, содержащий `user_id` и `event_type`
        :return: True — если запрос разрешён, False — если лимит превышен
        """
        identifier = f"{event.user_id}:{event.event_type}"
        logger.debug("Checking rate limit", identifier=identifier)
        try:
            return await self.rate_limiter.allow_request(identifier)
        except Exception as e:
            logger.warning("Rate limit check failed with error", error=str(e), identifier=identifier)
            return True