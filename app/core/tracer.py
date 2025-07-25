"""
Настройка OpenTelemetry трассировки.

Интегрирует Jaeger для распределенной трассировки запросов.
"""

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from .config import config


class TracerConfig:
    """Конфигурация трассировки."""

    def __init__(self):
        self.setup_tracing()

    def setup_tracing(self) -> None:
        """
        Инициализирует OpenTelemetry трассировку с Jaeger экспортёром.

        Создает провайдер трассировки и настраивает экспорт в Jaeger.
        """
        # Создаем ресурс с именем сервиса
        resource = Resource(attributes={
            SERVICE_NAME: config.service_name
        })

        jaeger_exporter = JaegerExporter(
            agent_host_name=config.jaeger.agent_host,
            agent_port=config.jaeger.agent_port,
        )

        provider = TracerProvider(resource=resource)

        processor = BatchSpanProcessor(jaeger_exporter)
        provider.add_span_processor(processor)

        # Устанавливаем провайдер как глобальный
        trace.set_tracer_provider(provider)

        # Инструментируем логирование для добавления trace_id
        LoggingInstrumentor().instrument(set_logging_format=True)


tracer_config = TracerConfig()