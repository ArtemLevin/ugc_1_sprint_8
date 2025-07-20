from app.repositories.redis_repository import RedisRepository
from app.models.event import Event
from app.utils.cache import RedisService


class RepositoryFactory:
    @staticmethod
    def create_event_cache(redis_service: RedisService) -> RedisRepository[Event]:
        return RedisRepository[Event](redis_service, Event)