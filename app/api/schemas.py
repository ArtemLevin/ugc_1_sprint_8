"""
Pydantic схемы для валидации данных API.

Определяют структуру входящих событий и обеспечивают валидацию.
"""

from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional
import re


class UserEventSchema(BaseModel):
    """
    Схема пользовательского события.

    Используется для валидации входящих данных через Pydantic.
    """

    user_id: str = Field(..., min_length=1, max_length=100,
                         description="Идентификатор пользователя")
    movie_id: str = Field(..., min_length=1, max_length=100,
                          description="Идентификатор фильма")
    event_type: str = Field(..., min_length=1, max_length=50,
                            description="Тип события (start, pause, stop, etc.)")
    timestamp: datetime = Field(...,
                                description="Временная метка события")

    class Config:
        """Конфигурация Pydantic модели."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        schema_extra = {
            "example": {
                "user_id": "user123",
                "movie_id": "movie456",
                "event_type": "start",
                "timestamp": "2024-01-01T12:00:00"
            }
        }

    @validator('user_id')
    def validate_user_id(cls, v):
        """Валидирует идентификатор пользователя."""
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('user_id must contain only alphanumeric characters, hyphens and underscores')
        return v

    @validator('event_type')
    def validate_event_type(cls, v):
        """Валидирует тип события."""
        allowed_types = {'start', 'pause', 'resume', 'stop', 'seek', 'buffering', 'error'}
        if v not in allowed_types:
            raise ValueError(f'event_type must be one of: {", ".join(allowed_types)}')
        return v

    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Валидирует временную метку."""
        if v > datetime.now():
            raise ValueError('timestamp cannot be in the future')
        return v


class HealthResponseSchema(BaseModel):
    """Схема ответа для health check."""

    status: str = Field(..., description="Статус сервиса")
    service: str = Field(..., description="Имя сервиса")
    version: Optional[str] = Field(None, description="Версия сервиса")


class MetricsResponseSchema(BaseModel):
    """Схема ответа для метрик."""

    events_processed: int = Field(0, description="Количество обработанных событий")
    errors_count: int = Field(0, description="Количество ошибок")
    uptime: str = Field("0s", description="Время работы сервиса")
    queue_length: Optional[int] = Field(None, description="Длина очереди событий")


class DLQStatusSchema(BaseModel):
    """Схема ответа для статуса DLQ."""

    dlq_length: int = Field(0, description="Количество сообщений в DLQ")
    dlq_enabled: bool = Field(True, description="DLQ включен")