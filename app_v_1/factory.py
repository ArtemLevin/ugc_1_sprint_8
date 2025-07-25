from dependency_injector.wiring import inject, Provide
from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from redis import from_url
from app_v_1.core.config import settings
from app_v_1.core.logger import get_logger
from app_v_1.core.tracing import setup_tracing
from app_v_1.core.health import register_health_check
from app_v_1.containers import Container
import logging
import asyncio


logger = get_logger(__name__)

class AppFactory:
    def __init__(self):
        self.app: Flask = Flask(__name__)
        self.container = Container()
        self.container.wire(packages=["app_v_1"])

        self.limiter = Limiter(
            app=self.app,
            key_func=get_remote_address,
            storage_uri=settings.redis.url,
            default_limits=["200 per minute", "50 per second"]
        )

        # Логгер будет инициализирован после configure_logging
        self.logger = None

    def configure_logging(self):
        logging.basicConfig(
            level=logging.DEBUG if settings.app.debug else logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.logger = get_logger(__name__)

    def configure_tracing(self):
        try:
            setup_tracing(self.app)
            self.logger.info("Tracing configured")
        except Exception as e:
            self.logger.warning("Tracing setup failed", error=str(e))

    def register_health_check(self):
        register_health_check(self.app)

    def register_routes(self):
        from app_v_1.routes import register_routes
        register_routes(self.app, self.limiter)

    def register_lifecycle_hooks(self):
        # флаг, чтобы инициализировать только один раз
        self._initialized = False

        @self.app.before_request
        def initialize():
            # выполняем только перед первым запросом
            if not self._initialized:
                self._initialized = True
                logger.info("Initializing application (lazy before_first_request)")
                try:
                    asyncio.run(self.container.redis_service().get_client().ping())
                    asyncio.run(self.container.kafka_producer().start())
                    logger.info("Initialization successful")
                except Exception as e:
                    logger.error("Initialization failed", error=str(e))
                    # можно пробросить, а можно дать сервису подождать,
                    # ошибки проявятся на health-чеке или при отправке

        @self.app.teardown_appcontext
        def shutdown(exc):
            logger.info("Shutting down application")
            asyncio.run(self.container.kafka_producer().stop())
            asyncio.run(self.container.redis_service().close())

    def build(self) -> Flask:
        self.configure_logging()
        self.configure_tracing()
        register_health_check(self.app)
        self.register_routes()
        self.register_lifecycle_hooks()
        self.app.container = self.container
        return self.app


def create_app():
    return AppFactory().build()