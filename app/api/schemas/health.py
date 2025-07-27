from pydantic import BaseModel, Field


class HealthResponseSchema(BaseModel):
    """Схема ответа для health check."""

    status: str = Field(..., description="Статус сервиса")
    service: str = Field(..., description="Имя сервиса")
    version: str | None = Field(None, description="Версия сервиса")
