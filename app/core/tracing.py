"""
Модуль для настройки и использования трассировки (tracing) в приложении.

Содержит функции для инициализации OpenTelemetry, настройки экспортера и добавления
контекста событий в трассы.
"""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from app.core.config import settings
from app.core.logger import get_logger
from app.models.event import Event

logger = get_logger(__name__)


class TracingService:
    """
    Сервис для настройки и управления трассировкой (tracing) в приложении.

    Attributes:
        endpoint: Конечная точка OTLP-экспортера.
    """

    def __init__(self, endpoint: str = settings.tracing.otel_exporter_otlp_endpoint):
        self.endpoint = endpoint
        self._tracer_provider = None
        self._tracer = None
        logger.info("TracingService initialized", endpoint=endpoint)

    def setup_tracing(self, app: FlaskInstrumentor) -> None:
        """
        Настраивает трассировку для Flask-приложения.

        Args:
            app: Flask-приложение, для которого нужно настроить трассировку.
        """
        logger.info("Setting up tracing", endpoint=self.endpoint)
        try:
            self._tracer_provider = TracerProvider()
            exporter = OTLPSpanExporter(endpoint=self.endpoint)
            self._tracer_provider.add_span_processor(
                BatchSpanProcessor(exporter)
            )
            trace.set_tracer_provider(self._tracer_provider)
            FlaskInstrumentor.instrument_app(app)
            self._tracer = trace.get_tracer(__name__)
            logger.info("Tracing successfully configured")
        except Exception as e:
            logger.error("Failed to configure tracing", error=str(e))
            raise

    def create_event_span(self, event: Event) -> None:
        """
        Создает span для события и добавляет к нему атрибуты.

        Args:
            event: Событие, для которого нужно создать span.
        """
        if not self._tracer:
            logger.warning("Tracer is not initialized")
            return

        logger.debug("Creating span for event", event_type=event.event_type, user_id=str(event.user_id))
        with self._tracer.start_as_current_span("track_event") as span:
            span.set_attribute("event.type", event.event_type)
            span.set_attribute("user.id", str(event.user_id))
            span.set_attribute("event.timestamp", event.timestamp.isoformat())
            span.set_attribute("event.session_id", str(event.session_id) if event.session_id else "none")
            logger.debug("Span created with attributes", event_type=event.event_type)