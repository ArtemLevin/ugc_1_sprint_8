from flask import request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from dependency_injector.wiring import inject, Provide

logger = get_logger(__name__)

@inject
async def track_event_route(event_processor=Provide['event_processor']):
    data = request.get_json()
    try:
        event = Event(**data)
    except Exception as e:
        logger.error("Invalid event format",
                     error=str(e),
                     raw_data=data)
        return jsonify({"error": "Invalid input"}), 400

    return await event_processor.process_event(event)