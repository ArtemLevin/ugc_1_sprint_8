from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone
import re


class BaseEventSchema(BaseModel):
    """
    Базовая схема пользовательского события.

    Используется для валидации входящих данных через Pydantic.
    """

    user_id: str = Field(..., min_length=1, max_length=100, description="Идентификатор пользователя")
    event_id: str = Field(..., min_length=1, max_length=100, description="Идентификатор события")
    timestamp: datetime = Field(..., description="Временная метка события")

    class Config:
        """Конфигурация Pydantic модели."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        json_schema_extra = {
            "example": {
                "user_id": "user123",
                "movie_id": "movie456",
                "event_type": "start",
                "timestamp": "2024-01-01T12:00:00"
            }
        }

    @field_validator('user_id')
    def validate_user_id(cls, v):
        """Валидирует идентификатор пользователя."""
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('user_id must contain only alphanumeric characters, hyphens and underscores')
        return v

    @field_validator('timestamp')
    def validate_timestamp(cls, v):
        """Валидирует временную метку."""
        if v > datetime.now(timezone.utc):
            raise ValueError('timestamp cannot be in the future')
        return v
