"""
Модуль маршрутов Flask для обработки пользовательских событий.

Содержит обработчики для `/api/v1/events/track`.
"""

from flask import Flask, request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.core.errors import AppError
from dependency_injector.wiring import inject, Provide
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import structlog

logger = get_logger(__name__)

def register_routes(app: Flask) -> None:
    # Добавляем rate limit к маршруту
    app.add_url_rule(
        "/api/v1/events/track",
        "track_event",
        inject(track_event_route)(event_processor=Provide['event_processor']),
        methods=["POST"],
        # Пример: 100 запросов в минуту, 20 в секунду
        limiter=Limiter(key_func=get_remote_address, default_limits=["100 per minute", "20 per second"])
    )


async def track_event_route(event_processor):
    try:
        data = request.get_json()
        event = Event(**data)
        result, status = await event_processor.process_event(event)
        return jsonify(result), status
    except AppError as e:
        return e.to_response()
    except Exception as e:
        logger.error("Unexpected", error=str(e), exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

    except AppError as e:
        logger.warning(
            "Application error occurred",
            error=e.message,
            status_code=e.status_code,
            event_type=event.event_type if 'event' in locals() else None,
            user_id=str(event.user_id) if 'event' in locals() else None
        )
        return e.to_response()

    except Exception as e:
        logger.error(
            "Unexpected error during event processing",
            error=str(e),
            event_type=event.event_type if 'event' in locals() else None,
            user_id=str(event.user_id) if 'event' in locals() else None,
            exc_info=True
        )
        return jsonify({"error": "Internal server error"}), 500