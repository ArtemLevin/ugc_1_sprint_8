from pydantic import BaseModel, Field


class DLQStatusSchema(BaseModel):
    """Схема ответа для статуса DLQ."""

    dlq_length: int = Field(0, description="Количество сообщений в DLQ")
    dlq_enabled: bool = Field(True, description="DLQ включен")