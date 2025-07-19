from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from app.core.logger import get_logger

logger = get_logger(__name__)

class RetryHandler:
    def __init__(
        self,
        max_retries=3,
        base_delay=1,
        max_delay=10,
        retryable_exceptions=(Exception,)
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.retryable_exceptions = retryable_exceptions

    def __call__(self, func):
        decorator = retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.base_delay, max=self.max_delay),
            retry=retry_if_exception_type(self.retryable_exceptions),
            before_sleep=self._log_retry
        )
        return decorator(func)

    def _log_retry(self, retry_state):
        attempt = retry_state.attempt_number
        exc = retry_state.outcome.exception()
        logger.warning(
            "Retrying operation",
            attempt=attempt,
            error=str(exc),
            func_name=retry_state.fn.__name__
        )