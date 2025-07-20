from flask import Flask
from flask.async import AsyncFlask
from app.core.config import settings
from app.core.logger import get_logger
from app.core.tracing import setup_tracing
from app.core.health import register_health_check
from app.routes import track_event_route
from app.containers import Container
import logging


def create_app():
    """
    Фабричная функция для создания и настройки Flask-приложения.

    Возвращает: AsyncFlask-приложение с внедрёнными зависимостями и настроенными хуками.
    """

    app = AsyncFlask(__name__)

    setup_tracing(app)

    register_health_check(app)

    logging.basicConfig(
        level=logging.DEBUG if settings.app.app_debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = get_logger(__name__)

    container = Container()

    app.container = container

    @app.route('/api/v1/events/track', methods=['POST'])
    async def track_event():
        return await track_event_route()

    @app.before_first_request
    async def initialize():
        """
        Инициализация сервисов при первом запросе.
        Подключаемся к Redis и запускаем Kafka-продюсера.
        """
        logger.info("Initializing application")
        try:
            redis_pool = app.container.redis_pool()
            await redis_pool.get_client()  # Проверяем подключение

            await app.container.kafka_producer().start()

            logger.info("Application initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize application", error=str(e))
            raise


    @app.teardown_appcontext
    async def shutdown(exception):
        """
        Очистка ресурсов при завершении работы приложения.
        Останавливаем Kafka и закрываем Redis-соединение.
        """
        logger.info("Shutting down application")
        await app.container.kafka_producer().stop()
        await app.container.redis_pool().close()

    return app