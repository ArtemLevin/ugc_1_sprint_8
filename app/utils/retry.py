from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from app.core.logger import get_logger
from functools import wraps
import logging

logger = get_logger(__name__)

class RetryHandler:
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 10.0,
        retryable_exceptions: tuple = (Exception,),
        log_level: int = logging.WARNING
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retryable_exceptions = retryable_exceptions
        self.log_level = log_level

    def __call__(self, func):
        @wraps(func)
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.base_delay, max=self.max_delay),
            retry=retry_if_exception_type(self.retryable_exceptions),
            before_sleep=self._get_before_sleep(),
            reraise=True
        )
        async def wrapped(*args, **kwargs):
            return await func(*args, **kwargs)

        return wrapped

    def _get_before_sleep(self):
        return before_sleep_log(logger, self.log_level, exc_info=True)