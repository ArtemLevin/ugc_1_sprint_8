from flask import request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.core.errors import AppError
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

    logger.info("Received event",
                event_type=event.event_type,
                user_id=str(event.user_id),
                session_id=str(event.session_id))

    try:
        result = await event_processor.process_event(event)

    except AppError as e:
        # Обработка всех пользовательских ошибок (RateLimitExceeded, DuplicateEvent, KafkaSendError)
        logger.warning("Application error occurred",
                       error=e.message,
                       status_code=e.status_code,
                       event_type=event.event_type,
                       user_id=str(event.user_id))
        return e.to_response()

    except Exception as e:
        # Обработка всех неожиданных ошибок
        logger.error("Unexpected error during event processing",
                     error=str(e),
                     event_type=event.event_type,
                     user_id=str(event.user_id),
                     exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

    logger.info("Event processed successfully",
                event_type=event.event_type,
                user_id=str(event.user_id))

    return jsonify(result[0]), result[1]