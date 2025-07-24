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
from app.containers import Container
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
    @limiter.limit("100 per minute;20 per second")  # ✅ Используем глобальный лимитер
    @inject
    async def track_event_route(event_processor=Provide['event_processor']):
        try:
            data = request.get_json()
            if not
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
                status_code=e._get_status_code(),  # Вызываем метод для получения кода
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


async def track_event_route(event_processor):
    """
    Основной обработчик POST-запроса на /api/v1/events/track.

    Args:
        event_processor: Сервис для обработки события (внедряется через DI).

    Returns:
        Flask-ответ в формате (jsonify(result), status_code)

    Логика:
    1. Получает JSON из тела запроса;
    2. Создаёт Pydantic-модель Event — при этом срабатывает валидация;
    3. Передаёт событие в EventProcessor — запускается цепочка обработки;
    4. Возвращает ответ клиенту.

    Важно: функция асинхронная — поддерживает high-load.
    """
    try:
        data = request.get_json()

        # Создаём Pydantic-модель — при этом срабатывает валидация:
        # - user_id: UUID
        # - timestamp: datetime
        # - event_type: StrEnum
        # - meta: Dict
        # Если данные невалидны — выбрасывается ValidationError
        event = Event(**data)

        # Передаём событие в EventProcessor
        # Здесь запускается цепочка: дубли → рейт-лимит → Kafka
        result, status = await event_processor.process_event(event)

        return jsonify(result), status

    except ValidationError as e:
        logger.warning("Invalid event data", errors=e.errors())
        return jsonify({"error": "Invalid data", "details": e.errors()}), 422

    except AppError as e:
        # Ожидаемые прикладные ошибки: дубликат, рейт-лимит
        # Они уже логируются внутри to_response()
        return e.to_response()

    except Exception as e:
        # Неожиданные ошибки — логируем с traceback
        # Это может быть ошибка валидации, сериализации, подключения к Redis и т.д.
        logger.error(
            "Unexpected error during event processing",
            error=str(e),
            event_type=event.event_type if 'event' in locals() else None,
            user_id=str(event.user_id) if 'event' in locals() else None,
            exc_info=True  # включает traceback
        )
        return jsonify({"error": "Internal server error"}), 500
