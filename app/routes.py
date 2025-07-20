"""
Модуль маршрутов Flask для обработки пользовательских событий.

Содержит обработчики для `/api/v1/events/track`.
"""

from flask import Flask, request, jsonify
from app.models.event import Event
from app.core.logger import get_logger
from app.core.errors import AppError
from app.services.event_processor import EventProcessor
from dependency_injector.wiring import inject, Provide
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from typing import Any, Dict, Tuple
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


async def track_event_route(event_processor: EventProcessor) -> Tuple[Dict[str, Any], int]:
    """
    Обработчик маршрута `/api/v1/events/track`.

    Принимает событие, валидирует его и передает на обработку в `event_processor`.

    Returns:
        JSON-ответ и HTTP-статус-код.
    """
    try:
        # Получаем данные из запроса
        data = request.get_json()
        logger.info("Received event data", data=data)

        # Валидируем и создаем событие
        event = Event(**data)
        logger.info(
            "Received event",
            event_type=event.event_type,
            user_id=str(event.user_id),
            session_id=str(event.session_id)
        )

        # Обрабатываем событие
        result = await event_processor.process_event(event)
        logger.info(
            "Event processed successfully",
            event_type=event.event_type,
            user_id=str(event.user_id)
        )

        return jsonify(result[0]), result[1]

    except AppError as e:
        # Обработка пользовательских ошибок
        logger.warning(
            "Application error occurred",
            error=e.message,
            status_code=e.status_code,
            event_type=event.event_type if 'event' in locals() else None,
            user_id=str(event.user_id) if 'event' in locals() else None
        )
        return e.to_response()

    except Exception as e:
        # Обработка неожиданных ошибок
        logger.error(
            "Unexpected error during event processing",
            error=str(e),
            event_type=event.event_type if 'event' in locals() else None,
            user_id=str(event.user_id) if 'event' in locals() else None,
            exc_info=True
        )
        return jsonify({"error": "Internal server error"}), 500