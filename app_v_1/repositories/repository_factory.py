from app_v_1.repositories.redis_repository import RedisRepository
from app_v_1.models.event import Event
from app_v_1.utils.cache import RedisService


class RepositoryFactory:
    @staticmethod
    def create_event_cache(redis_service: RedisService) -> RedisRepository[Event]:
        return RedisRepository[Event](redis_service, Event)