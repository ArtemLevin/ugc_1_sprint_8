"""
Модуль, содержащий классы ошибок и исключений для приложения.

Определяет иерархию прикладных ошибок, которые могут возникнуть в ходе выполнения запросов.
"""

from flask import jsonify
from typing import Optional, Any, Dict
from dataclasses import dataclass
import logging
from http import HTTPStatus

logger = logging.getLogger(__name__)


@dataclass
class ErrorDetails:
    """
    Класс для хранения детальной информации об ошибке.

    Attributes:
        code: Код ошибки (например, "duplicate_event").
        message: Человекочитаемое описание ошибки.
        retryable: Можно ли повторить операцию.
        context: Дополнительные данные об ошибке.
    """
    code: str
    message: str
    retryable: bool = False
    context: Dict[str, Any] = None


class AppError(Exception):
    """
    Базовый класс для всех прикладных ошибок.

    Attributes:
        error: Объект с детальной информацией об ошибке.
    """

    def __init__(self, error: ErrorDetails):
        self.error = error
        super().__init__(error.message)

    def to_response(self):
        """
        Преобразует ошибку в Flask-ответ с JSON-телом и статус-кодом.

        Returns:
            кортеж (JSON-ответ, HTTP-статус)
        """
        logger.warning(
            "Application error occurred",
            extra={
                "error_code": self.error.code,
                "error_message": self.error.message,
                "error_context": self.error.context
            }
        )

        response = {
            "error": {
                "code": self.error.code,
                "message": self.error.message,
            }
        }

        if self.error.context:
            response["error"]["context"] = self.error.context

        return jsonify(response), self._get_status_code()

    def _get_status_code(self) -> int:
        """Возвращает соответствующий HTTP-статус для ошибки."""
        raise NotImplementedError("Subclasses must implement _get_status_code()")


class ClientError(AppError):
    """Базовый класс для ошибок, вызванных клиентом."""

    def _get_status_code(self) -> int:
        return HTTPStatus.BAD_REQUEST


class ServerError(AppError):
    """Базовый класс для ошибок сервера."""

    def _get_status_code(self) -> int:
        return HTTPStatus.INTERNAL_SERVER_ERROR


class RateLimitExceeded(ServerError):
    """Ошибка, возникающая при превышении лимита запросов."""

    def __init__(self, identifier: str, retry_after: Optional[int] = None):
        self.identifier = identifier
        self.retry_after = retry_after
        error = ErrorDetails(
            code="rate_limit_exceeded",
            message=f"Rate limit exceeded for {identifier}",
            retryable=True,
            context={
                "identifier": identifier,
                "retry_after": retry_after
            }
        )
        super().__init__(error)


class DuplicateEvent(ClientError):
    """Ошибка, возникающая при попытке отправить дубликат события."""

    def __init__(self, event_id: str):
        error = ErrorDetails(
            code="duplicate_event",
            message=f"Event with ID {event_id} is a duplicate",
            context={"event_id": event_id}
        )
        super().__init__(error)


class KafkaSendError(ServerError):
    """Ошибка, возникающая при неудачной отправке события в Kafka."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        error = ErrorDetails(
            code="kafka_send_error",
            message=message,
            retryable=True,
            context={
                "original_error": str(original_error) if original_error else None
            }
        )
        super().__init__(error)


class ConfigurationError(ServerError):
    """Ошибка конфигурации приложения."""

    def __init__(self, message: str, component: str):
        error = ErrorDetails(
            code="configuration_error",
            message=message,
            context={"component": component}
        )
        super().__init__(error)