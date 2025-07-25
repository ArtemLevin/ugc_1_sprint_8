"""
Модуль маршрутов Flask для обработки пользовательских событий.
Содержит обработчики для `/api/v1/events/track`.
"""

from flask import Flask, request, jsonify
from app_v_1.models.event import Event
from app_v_1.core.logger import get_logger
from app_v_1.core.errors import AppError
from dependency_injector.wiring import inject, Provide
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import structlog
from pydantic import ValidationError

logger = get_logger(__name__)


def register_routes(app: Flask, limiter: Limiter) -> None:
    """
    Регистрирует маршруты приложения.

    Args:
        app: Экземпляр Flask-приложения.
        limiter: Глобальный экземпляр Flask-Limiter.
    """

    @app.route("/api/v1/events/track", methods=["POST"])
    @limiter.limit("100 per minute;20 per second")
    @inject
    async def track_event_route(event_processor=Provide['event_processor']):
        try:
            data = request.get_json()
            if not data:
                logger.warning("Empty request body")
                return jsonify({"error": "Empty request body"}), 400

            event = Event(**data)
            result, status = await event_processor.process_event(event)
            return jsonify(result), status

        except ValidationError as e:
            logger.warning("Invalid event data", errors=e.errors())
            return jsonify({"error": "Invalid data", "details": e.errors()}), 422

        except AppError as e:
            logger.warning(
                "Application error occurred",
                error=e.error.code,
                message=e.error.message,
                status_code=e._get_status_code(),
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


