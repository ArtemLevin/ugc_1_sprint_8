from enum import StrEnum
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, model_validator

class EventType(StrEnum):
    CLICK = "click"
    PAGE_VIEW = "page_view"
    VIDEO_QUALITY_CHANGE = "video_quality_change"
    VIDEO_WATCHED_TO_END = "video_watched_to_end"
    SEARCH_FILTER_USED = "search_filter_used"

class Event(BaseModel):
    user_id: UUID
    event_type: EventType
    timestamp: datetime
    session_id: Optional[UUID] = None
    meta: Dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_event_type_with_meta(self) -> "Event":
        required_meta_fields = {
            EventType.VIDEO_QUALITY_CHANGE: ["from_quality", "to_quality"],
            EventType.VIDEO_WATCHED_TO_END: ["movie_id", "watch_time_seconds"],
            EventType.SEARCH_FILTER_USED: ["filter_type", "value"],
            EventType.CLICK: ["element_type", "element_id"],
            EventType.PAGE_VIEW: ["page", "duration_seconds"],
        }
        required_fields = required_meta_fields.get(self.event_type)
        if required_fields:
            for field in required_fields:
                if field not in self.meta:
                    raise ValueError(f"Field '{field}' is required in 'meta' for event type '{self.event_type}'")
        return self