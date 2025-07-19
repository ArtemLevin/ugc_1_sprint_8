import structlog
from opentelemetry import trace
from app.core.config import settings


def get_logger(name: str):
    tracer = trace.get_tracer(name)
    current_span = tracer.get_current_span()

    trace_id = "unknown"
    span_id = "unknown"

    if current_span:
        context = current_span.context
        trace_id = format(context.trace_id, '032x') if context.trace_id else "unknown"
        span_id = format(context.span_id, '016x') if context.span_id else "unknown"

    return structlog.get_logger(name).bind(
        trace_id=trace_id,
        span_id=span_id,
        environment=settings.app.app_env,
        service_name=settings.app.app_name
    )