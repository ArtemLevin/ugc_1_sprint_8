from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from app.core.config import settings

def setup_tracing(app):
    provider = TracerProvider()
    exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    FlaskInstrumentor.instrument_app(app)

def add_event_attributes(event):
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("track_event") as span:
        span.set_attribute("event.type", event.event_type)
        span.set_attribute("user.id", str(event.user_id))
        span.set_attribute("event.timestamp", event.timestamp.isoformat())