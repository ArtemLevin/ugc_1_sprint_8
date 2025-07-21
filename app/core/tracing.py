from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from flask import Flask

from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)


class TracingService:
    def __init__(self, endpoint: str = settings.tracing.otel_exporter_otlp_endpoint):
        self.endpoint = endpoint
        self._tracer_provider = None
        self._tracer = None

    def setup_tracing(self, app: Flask) -> None:
        logger.info("Setting up tracing", endpoint=self.endpoint)
        # все нужные объекты импортированы сверху
        provider = TracerProvider()
        exporter = OTLPSpanExporter(endpoint=self.endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        FlaskInstrumentor().instrument_app(app)
        self._tracer = trace.get_tracer(__name__)
        logger.info("Tracing successfully configured")


# Добавляем эту функцию, чтобы в factory.py было из чего импортить
_tracer_service = TracingService()
def setup_tracing(app: Flask) -> None:
    """
    Функция-обёртка для AppFactory.
    """
    try:
        _tracer_service.setup_tracing(app)
    except Exception as e:
        # сюда попадёт, если, к примеру, не смогли соединиться с Jaeger
        logger.warning("Tracing setup failed", error=str(e))