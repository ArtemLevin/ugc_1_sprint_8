from flask import jsonify
from typing import Optional, Any


class AppError(Exception):

    status_code = 500  
    message = "Internal error" 
    def __init__(
            self,
            message: str | None = None,
            status_code: int | None = None,
            extra: dict[str, Any] | None = None
    ):
        if message:
            self.message = message
        if status_code:
            self.status_code = status_code
        self.extra = extra or {}

        super().__init__(self.message)

    def to_response(self):
        """
        Преобразует ошибку в Flask-ответ с JSON-телом и статус-кодом.

        :return: кортеж (JSON-ответ, HTTP-статус)
        """
        response = {
            "error": self.message,
            **self.extra  
        }
        return jsonify(response), self.status_code


class RateLimitExceeded(AppError):
    status_code = 429
    message = "Too many requests"


class DuplicateEvent(AppError):
    status_code = 409
    message = "Event is duplicate"


class KafkaSendError(AppError):
    status_code = 503
    message = "Failed to send event"

    def __init__(self, message: str | None = None, status_code: int | None = None):
        super().__init__(message=message, status_code=status_code, extra={"retry_later": True})