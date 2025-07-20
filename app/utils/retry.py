"""
Модуль для реализации retry-логики с использованием библиотеки tenacity.

Содержит класс `RetryHandler`, предоставляющий гибкую конфигурацию retry-поведения.
"""

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    retry_if_exception_type,
    RetryError
)
from app.core.logger import get_logger
from typing import Callable, Type, Optional, Any, Union, Tuple
from functools import wraps
import logging

logger = get_logger(__name__)


class RetryHandler:
    """
    Класс для настройки retry-логики с гибкими параметрами.

    Attributes:
        max_retries: Максимальное количество попыток.
        base_delay: Начальная задержка между попытками.
        max_delay: Максимальная задержка между попытками.
        retryable_exceptions: Кортеж исключений, при которых возможен retry.
        log_level: Уровень логирования при ожидании следующей попытки.
        before_sleep: Функция, вызываемая перед следующей попыткой.
    """

    def __init__(
            self,
            max_retries: int = 3,
            base_delay: float = 1.0,
            max_delay: float = 10.0,
            retryable_exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (Exception,),
            log_level: int = logging.WARNING,
            before_sleep: Optional[Callable] = None
    ):
        """
        Инициализирует RetryHandler с указанными параметрами.

        Args:
            max_retries: Максимальное количество попыток.
            base_delay: Начальная задержка между попытками.
            max_delay: Максимальная задержка между попытками.
            retryable_exceptions: Исключения, при которых возможен retry.
            log_level: Уровень логирования для retry-сообщений.
            before_sleep: Дополнительная функция, вызываемая перед retry.
        """
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

        # Поддержка как одного исключения, так и кортежа исключений
        if isinstance(retryable_exceptions, type) and issubclass(retryable_exceptions, Exception):
            self.retryable_exceptions = (retryable_exceptions,)
        else:
            self.retryable_exceptions = retryable_exceptions

        self.log_level = log_level
        self.before_sleep = before_sleep or self._default_before_sleep()

    def _default_before_sleep(self) -> Callable:
        """
        Возвращает функцию по умолчанию для вызова перед retry.

        Returns:
            Функцию по умолчанию для логирования перед retry.
        """
        return before_sleep_log(logger, self.log_level, exc_info=True)

    def __call__(self, func: Callable) -> Callable:
        """
        Применяет retry-логику к указанной функции.

        Args:
            func: Функция или метод, к которому нужно применить retry-логику.

        Returns:
            Обёрнутая функция с retry-логикой.
        """

        @wraps(func)
        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.base_delay, max=self.max_delay),
            retry=retry_if_exception_type(self.retryable_exceptions),
            before_sleep=self.before_sleep,
            reraise=True
        )
        async def wrapped(*args, **kwargs) -> Any:
            """
            Обёрнутая функция с retry-логикой.

            Args:
                *args: Позиционные аргументы исходной функции.
                **kwargs: Именованные аргументы исходной функции.

            Returns:
                Результат выполнения функции.

            Raises:
                RetryError: Если все попытки завершились неудачей.
            """
            return await func(*args, **kwargs)

        return wrapped

    def get_retry_decorator(self) -> Callable:
        """
        Возвращает готовый retry-декоратор для использования в других частях кода.

        Returns:
            Декоратор с настроенной retry-логикой.
        """
        return self.__call__