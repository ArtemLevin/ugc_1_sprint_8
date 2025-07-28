"""
Модели данных для ClickHouse.

Определяют структуру таблиц и типы данных.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class UserEventModel:
    """
    Модель пользовательского события.

    Используется для типизации данных перед вставкой в ClickHouse.
    """
    user_id: str
    movie_id: str
    event_type: str
    timestamp: datetime

    def to_tuple(self) -> tuple:
        """Преобразует событие в кортеж для вставки в ClickHouse."""
        return (self.user_id, self.movie_id, self.event_type, self.timestamp)

    @classmethod
    def from_dict(cls, data: dict) -> 'UserEventModel':
        """
        Создает экземпляр UserEventModel из словаря.

        Args:
             Словарь с данными события

        Returns:
            UserEventModel: Экземпляр события
        """
        timestamp_dt = data['timestamp']
        if isinstance(timestamp_dt, str):
            try:
                timestamp_dt = datetime.fromisoformat(timestamp_dt)
            except ValueError as e:
                raise ValueError(f"Invalid timestamp format: {timestamp_dt}") from e

        return cls(
            user_id=data['user_id'],
            movie_id=data['movie_id'],
            event_type=data['event_type'],
            timestamp=timestamp_dt
        )


@dataclass
class TableInfo:
    """Информация о таблице ClickHouse."""

    row_count: int
    first_event: str
    last_event: str


@dataclass
class QueryResult:
    """Результат выполнения SQL запроса."""

    rows: list
    columns: list