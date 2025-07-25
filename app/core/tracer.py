"""
Настройка OpenTelemetry трассировки.
Интегрирует Jaeger для распределенной трассировки запросов.
"""

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

class TracerConfig:
    """Конфигурация трассировки."""
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.setup_tracing()

    def setup_tracing(self) -> None:
        """
        Инициализирует OpenTelemetry трассировку с Jaeger экспортёром.
        Создает провайдер трассировки и настраивает экспорт в Jaeger.
        """
        # Создаем ресурс с именем сервиса
        resource = Resource(attributes={
            SERVICE_NAME: self.service_name
        })

        jaeger_exporter = JaegerExporter()

        provider = TracerProvider(resource=resource)
        
        processor = BatchSpanProcessor(jaeger_exporter)
        provider.add_span_processor(processor)
        
        trace.set_tracer_provider(provider)
