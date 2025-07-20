from dependency_injector.wiring import inject, Provide
from flask import Flask
from app.core.config import settings
from app.core.logger import get_logger
from app.core.tracing import setup_tracing
from app.core.health import register_health_check
from app.routes import track_event_route
from app.containers import Container
import logging

logger = get_logger(__name__)


class AppFactory:
    def __init__(self):
        self.app: Flask = Flask(__name__)
        # Создаём DI-контейнер
        self.container = Container()
        # Привязываем провайдеры к модулям с @inject/Provide
        self.container.wire(packages=["app"])

    def configure_logging(self):
        logging.basicConfig(
            level=logging.DEBUG if settings.app.debug else logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    def configure_tracing(self):
        setup_tracing(self.app)

    def register_health_check(self):
        register_health_check(self.app)

    def register_routes(self):
        @self.app.route("/api/v1/events/track", methods=["POST"])
        @inject
        async def track_event(
            event_processor=Provide[Container.event_processor]
        ):
            return await track_event_route(event_processor=event_processor)

    def register_lifecycle_hooks(self):
        @self.app.before_first_request
        async def initialize():
            logger.info("Initializing application")
            try:
                # Проверяем Redis и Kafka
                await self.container.redis_service().get_client().ping()
                await self.container.kafka_producer().start()
                logger.info("Initialization successful")
            except Exception as e:
                logger.error("Initialization failed", error=str(e))
                raise

        @self.app.teardown_appcontext
        async def shutdown(exc):
            logger.info("Shutting down application")
            await self.container.kafka_producer().stop()
            await self.container.redis_service().close()

    def build(self) -> Flask:
        self.configure_logging()
        self.configure_tracing()
        self.register_health_check()
        self.register_routes()
        self.register_lifecycle_hooks()
        # Доступ к контейнеру из anywhere: app.container
        self.app.container = self.container
        return self.app


def create_app():
    return AppFactory().build()