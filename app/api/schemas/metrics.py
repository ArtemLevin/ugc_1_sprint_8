from pydantic import BaseModel, Field


class MetricsResponseSchema(BaseModel):
    """Схема ответа для метрик."""

    events_processed: int = Field(0, description="Количество обработанных событий")
    errors_count: int = Field(0, description="Количество ошибок")
    uptime: str = Field("0s", description="Время работы сервиса")
    queue_length: int | None = Field(None, description="Длина очереди событий")