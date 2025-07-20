"""
Модуль для настройки и получения логгеров с контекстной информацией.

Содержит функции для создания логгеров с автоматическим добавлением контекста,
включая trace_id и span_id из OpenTelemetry.
"""

import structlog
from opentelemetry import trace
from app.core.config import settings
from typing import Dict
import logging
from opentelemetry.trace import SpanContext, TraceFlags


def _default_logger_factory() -> logging.Logger:
    """
    Фабрика для создания стандартного логгера.

    Returns:
        Новый логгер с базовой конфигурацией.
    """
    logger = logging.getLogger()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logger


def _get_trace_context() -> Dict[str, str]:
    """
    Получает текущий контекст трассировки из OpenTelemetry.

    Returns:
        Словарь с trace_id и span_id или значения по умолчанию.
    """
    current_span = trace.get_current_span()
    trace_id = "unknown"
    span_id = "unknown"

    if current_span and current_span.context and current_span.context.trace_flags & TraceFlags.SAMPLED:
        span_context: SpanContext = current_span.context
        if span_context.trace_id:
            trace_id = format(span_context.trace_id, '032x')
        if span_context.span_id:
            span_id = format(span_context.span_id, '016x')

    return {
        "trace_id": trace_id,
        "span_id": span_id
    }


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """
    Возвращает настроенный логгер с контекстной информацией.

    Args:
        name: Имя логгера. Если не указано, используется имя модуля.

    Returns:
        Настраиваемый логгер с привязанными контекстными данными.
    """
    # Получаем контекст трассировки
    trace_context = _get_trace_context()

    # Создаем логгер с базовым контекстом
    return structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ],
        logger_factory=_default_logger_factory,
        wrapper_class=structlog.BoundLogger,
        cache_logger_on_first_use=True,
    ).get_logger(name).bind(
        trace_id=trace_context["trace_id"],
        span_id=trace_context["span_id"],
        environment=settings.app.environment,
        service_name=settings.app.name
    )