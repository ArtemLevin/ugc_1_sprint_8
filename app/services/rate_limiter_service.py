from app.core.logger import get_logger

logger = get_logger(__name__)

class RateLimiterService:
    def __init__(self, rate_limiter):
        self.rate_limiter = rate_limiter

    async def check(self, event):
        identifier = f"{event.user_id}:{event.event_type}"
        return await self.rate_limiter.allow_request(identifier)