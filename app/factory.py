"""
Модуль для создания и настройки Flask-приложения.

Содержит фабричную функцию `create_app` для инициализации приложения с внедрением зависимостей,
настройкой логирования, трассировки и обработчиков запросов.
"""

from flask import Flask, AsyncFlask
from app.core.config import settings
from app.core.logger import get_logger
from app.core.tracing import setup_tracing
from app.core.health import register_health_check
from app.routes import track_event_route
from app.containers import Container
import logging

logger = get_logger(__name__)


class AppFactory:
    """
    Фабрика для создания и настройки Flask-приложения.

    Attributes:
        app: AsyncFlask-приложение.
        container: DI-контейнер с зависимостями.
    """

    def __init__(self):
        self.app: AsyncFlask = AsyncFlask(__name__)
        self.container: Container = Container()
        self.logger = get_logger(__name__)

    def configure_tracing(self) -> None:
        """
        Настраивает трассировку с использованием OpenTelemetry.
        """
        setup_tracing(self.app)

    def register_routes(self) -> None:
        """
        Регистрирует маршруты в приложении.
        """

        @self.app.route('/api/v1/events/track', methods=['POST'])
        async def track_event():
            return await track_event_route()

    def register_health_check(self) -> None:
        """
        Регистрирует эндпоинт /health для проверки состояния сервиса.
        """
        register_health_check(self.app)

    def configure_logging(self) -> None:
        """
        Настраивает базовое логирование.
        """
        logging.basicConfig(
            level=logging.DEBUG if settings.app.debug else logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def register_initialization_hook(self) -> None:
        """
        Регистрирует хук для инициализации сервисов при первом запросе.

        Подключается к Redis и запускает Kafka-продюсера.
        """

        @self.app.before_first_request
        async def initialize():
            self.logger.info("Initializing application")
            try:
                redis_pool = self.container.redis_pool()
                await redis_pool.get_client()  # Проверяем подключение
                await self.container.kafka_producer().start()
                self.logger.info("Application initialized successfully")
            except Exception as e:
                self.logger.error("Failed to initialize application", error=str(e))
                raise

    def register_shutdown_hook(self) -> None:
        """
        Регистрирует хук для очистки ресурсов при завершении работы приложения.

        Останавливает Kafka и закрывает соединение с Redis.
        """

        @self.app.teardown_appcontext
        async def shutdown(exception):
            self.logger.info("Shutting down application")
            await self.container.kafka_producer().stop()
            await self.container.redis_pool().close()

    def build(self) -> AsyncFlask:
        """
        Создаёт и настраивает приложение.

        Returns:
            Настроенное AsyncFlask-приложение.
        """
        self.configure_logging()
        self.configure_tracing()
        self.register_health_check()
        self.register_routes()
        self.register_initialization_hook()
        self.register_shutdown_hook()

        self.app.container = self.container
        return self.app


def create_app() -> AsyncFlask:
    """
    Фабричная функция для создания и настройки Flask-приложения.

    Returns:
        Настроенное AsyncFlask-приложение.
    """
    return AppFactory().build()