from pydantic import Field, TypeAdapter
from typing import Literal

from app.api.schemas.user_schema import BaseEventSchema

allowed_event_type = ["click", "page_view", "quality_change", "watched_to_end", "filter_used"]

class ClickEvent(BaseEventSchema):
    """Событие клика по элементу интерфейса."""
    event_type: Literal["click"] = Field("click", description="Тип события — click")
    target: str = Field(
        ..., description="Целевой элемент, по которому был произведен клик (например 'movie_description'"
    )


class PageViewEvent(BaseEventSchema):
    """Событие просмотра страницы."""
    event_type: Literal["page_view"] = Field("page_view", description="Тип события — page_view")
    page: str = Field(..., description="URL или идентификатор просматриваемой страницы")
    duration_seconds: int | None = Field(None, description="Продолжительность просмотра страницы в секундах")


class QualityChangeEvent(BaseEventSchema):
    """ Событие смены качества видео. """
    event_type: Literal["quality_change"] = Field("quality_change", description="Тип события — quality_change")
    from_quality: str = Field(..., description="Качество видео до переключения (например, '720p')")
    to_quality: str = Field(..., description="Качество видео после переключения (например, '1080p')")
    movie_id: str = Field(..., description="Идентификатор видео, в котором произошло изменение качества")


class WatchedToEndEvent(BaseEventSchema):
    """Событие полного просмотра видео."""
    event_type: Literal["watched_to_end"] = Field("watched_to_end", description="Тип события — watched_to_end")
    movie_id: str = Field(..., description="Идентификатор просмотренного до конца видео")


class FilterUseEvent(BaseEventSchema):
    """Событие использования фильтра при поиске."""
    event_type: Literal["filter_used"] = Field("filter_used", description="Тип события — filter_used")
    filter_name: str = Field(..., description="Название примененного фильтра (например, 'genre', 'rating')")
    filter_value: str = Field(..., description="Значение фильтра (например, 'action')")


Event = ClickEvent | PageViewEvent | FilterUseEvent | QualityChangeEvent | WatchedToEndEvent
UserEventSchema = TypeAdapter(Event)
