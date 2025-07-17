import structlog
from opentelemetry import trace

def get_logger(name: str):
    tracer = trace.get_tracer(name)
    return structlog.get_logger(name).bind(trace_id=str(tracer.get_current_span().context.trace_id))