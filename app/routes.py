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

logger = get_logger(__name__)


def register_routes(app: Flask) -> None:
    """
    Регистрирует маршрут /api/v1/events/track с помощью app.add_url_rule().

    Почему не @app.route()?
    - Позволяет более гибко управлять порядком регистрации;
    - Удобно для модульной архитектуры, где маршруты могут быть определены в другом месте;
    - Можно динамически изменять поведение (например, отключать маршруты).

    """
    # Добавляем rate limit к маршруту
    app.add_url_rule(
        "/api/v1/events/track",
        "track_event",
        # Используем dependency-injector для внедрения event_processor
        inject(track_event_route)(event_processor=Provide[Container.event_processor]),
        methods=["POST"],
        # Пример: 100 запросов в минуту, 20 в секунду
        # Но: это создаёт отдельный экземпляр Limiter, который не связан с глобальным
        limiter=Limiter(key_func=get_remote_address, default_limits=["100 per minute", "20 per second"])
    )


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
