from app.core.logger import get_logger
from typing import Any
from app.services.rate_limiter import RedisLeakyBucketRateLimiter

logger = get_logger(__name__)


class RateLimiterService:
    """
    Сервисный слой для проверки рейт-лимита.
    Является адаптером между бизнес-логикой (например, EventProcessor) и реализацией рейт-лимитера.

    Зачем нужен?
    - Чтобы инкапсулировать формирование ключа (user_id:event_type);
    - Чтобы централизовать логику обработки ошибок;
    - Чтобы обеспечить fail-open поведение при сбоях;
    - Чтобы упростить тестирование (можно подменить лимитер).
    """

    def __init__(self, rate_limiter: RedisLeakyBucketRateLimiter):
        """
        Инициализация сервиса.

        Args:
            rate_limiter: Реализация рейт-лимитера (например, RedisLeakyBucketRateLimiter).

        Примечание: принимает конкретный класс, а не интерфейс RateLimiter.
        Это ограничивает гибкость, но упрощает инъекцию в DI-контейнер.
        Лучше было бы зависеть от абстракции (RateLimiter), а не от реализации.
        """
        self.rate_limiter = rate_limiter

    async def check(self, event: Any) -> bool:
        """
        Проверяет, не превышен ли рейт-лимит для данного события.

        Алгоритм:
        1. Формирует ключ: {user_id}:{event_type}
        2. Передаёт ключ в rate_limiter.allow_request()
        3. В случае ошибки — возвращает True (fail-open)

        Args:
            event: Объект события, содержащий user_id и event_type.
                   Ожидается, что у объекта есть атрибуты user_id и event_type.

        Returns:
            True — если запрос разрешён (лимит не превышен или произошла ошибка);
            False — если лимит превышен.

        """
        # Формируем ключ для рейт-лимита: user_id:event_type
        # Это означает, что лимит применяется на комбинацию пользователя и типа события.
        # Например:
        #   - user-1:click — 100 req/min
        #   - user-1:page_view — 100 req/min
        #   - Эти лимиты независимы.
        identifier = f"{event.user_id}:{event.event_type}"

        logger.debug("Checking rate limit", identifier=identifier)

        try:
            return await self.rate_limiter.allow_request(identifier)

        except Exception as e:
            logger.warning(
                "Rate limit check failed with error",
                error=str(e),
                identifier=identifier
            )
            return True