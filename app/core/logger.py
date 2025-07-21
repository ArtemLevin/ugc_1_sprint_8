import logging
import structlog
from opentelemetry import trace
from opentelemetry.trace import SpanContext, TraceFlags
from typing import Dict, Optional
from app.core.config import settings

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=logging.getLogger,
    wrapper_class=structlog.BoundLogger,
    cache_logger_on_first_use=True,
)

def _get_trace_context() -> Dict[str, str]:
    span = trace.get_current_span()
    # сначала пробуем метод get_span_context()
    span_ctx = (
        span.get_span_context()
        if hasattr(span, "get_span_context")
        else getattr(span, "context", None)
    )

    trace_id = "unknown"
    span_id = "unknown"
    if isinstance(span_ctx, SpanContext) and (span_ctx.trace_flags & TraceFlags.SAMPLED):
        trace_id = format(span_ctx.trace_id, '032x')
        span_id  = format(span_ctx.span_id,  '016x')

    return {"trace_id": trace_id, "span_id": span_id}

def get_logger(name: Optional[str] = None) -> structlog.BoundLogger:
    """
    Возвращает structlog BoundLogger, привязанный к trace_id/span_id и данным из settings.
    """
    trace_ctx = _get_trace_context()
    # structlog.get_logger использует наш logger_factory и принимает имя
    logger = structlog.get_logger(name or __name__)
    return logger.bind(
        trace_id=trace_ctx["trace_id"],
        span_id=trace_ctx["span_id"],
        environment=settings.app.environment,
        service_name=settings.app.name
    )