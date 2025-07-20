"""
Модуль, содержащий модели данных для пользовательских событий.

Описывает структуру события и обеспечивает валидацию данных.
"""

from enum import StrEnum
from typing import Dict, Any, List, Optional, Protocol, ClassVar
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, model_validator
import logging

logger = logging.getLogger(__name__)


class EventType(StrEnum):
    """
    Перечисление типов событий, с которыми работает система.

    Примеры:
        - VIDEO_QUALITY_CHANGE: изменение качества видео
        - PAGE_VIEW: просмотр страницы
    """
    CLICK = "click"
    PAGE_VIEW = "page_view"
    VIDEO_QUALITY_CHANGE = "video_quality_change"
    VIDEO_WATCHED_TO_END = "video_watched_to_end"
    SEARCH_FILTER_USED = "search_filter_used"


class MetaValidator(Protocol):
    """
    Протокол для валидатора метаданных события.

    Реализации этого протокола определяют, какие поля в `meta` обязательны
    для конкретного типа события.
    """

    def validate(self, meta: Dict[str, Any]) -> None:
        """
        Проверяет, что метаданные соответствуют требованиям для данного типа события.

        Args:
            meta: Словарь с метаданными события.

        Raises:
            ValueError: Если одно из обязательных полей отсутствует.
        """
        ...


class BaseMetaValidator:
    """
    Базовая реализация валидатора метаданных.

    Может использоваться как суперкласс для валидаторов конкретных типов событий.
    """

    def __init__(self, required_fields: List[str]):
        """
        Args:
            required_fields: Список обязательных полей для проверки.
        """
        self.required_fields = required_fields

    def validate(self, meta: Dict[str, Any]) -> None:
        """
        Проверяет, что все обязательные поля присутствуют в `meta`.

        Args:
            meta: Словарь с метаданными события.

        Raises:
            ValueError: Если одно из обязательных полей отсутствует.
        """
        for field in self.required_fields:
            if field not in meta:
                raise ValueError(f"Field '{field}' is required in 'meta' for this event type")


class EventMetaValidator:
    """
    Фабрика валидаторов метаданных для событий.

    Создаёт соответствующий валидатор в зависимости от типа события.
    """

    _validators: ClassVar[Dict[EventType, MetaValidator]] = {
        EventType.VIDEO_QUALITY_CHANGE: BaseMetaValidator(["from_quality", "to_quality"]),
        EventType.VIDEO_WATCHED_TO_END: BaseMetaValidator(["movie_id", "watch_time_seconds"]),
        EventType.SEARCH_FILTER_USED: BaseMetaValidator(["filter_type", "value"]),
        EventType.CLICK: BaseMetaValidator(["element_type", "element_id"]),
        EventType.PAGE_VIEW: BaseMetaValidator(["page", "duration_seconds"]),
    }

    @classmethod
    def get_validator(cls, event_type: EventType) -> MetaValidator:
        """
        Возвращает валидатор для указанного типа события.

        Args:
            event_type: Тип события.

        Returns:
            Соответствующий валидатор метаданных.

        Raises:
            ValueError: Если валидатор для указанного типа события не найден.
        """
        validator = cls._validators.get(event_type)
        if not validator:
            logger.warning("No validator found for event type", extra={"event_type": event_type})
            return BaseMetaValidator([])
        return validator


class Event(BaseModel):
    """
    Модель пользовательского события.

    Attributes:
        user_id: Идентификатор пользователя.
        event_type: Тип события.
        timestamp: Время события.
        session_id: Необязательный идентификатор сессии.
        meta: Дополнительные данные о событии.
    """

    user_id: UUID
    event_type: EventType
    timestamp: datetime
    session_id: UUID | None = None
    meta: Dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_event_type_with_meta(self) -> "Event":
        """
        Проверяет, что метаданные соответствуют типу события.

        Raises:
            ValueError: Если одно из обязательных полей отсутствует в `meta`.
        """
        try:
            validator = EventMetaValidator.get_validator(self.event_type)
            validator.validate(self.meta)
            return self
        except ValueError as e:
            logger.error("Event validation failed", exc_info=True, extra={
                "event_type": self.event_type,
                "meta": self.meta
            })
            raise