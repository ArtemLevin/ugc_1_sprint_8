"""
Настройка структурированного логирования.

Использует structlog для создания JSON-логов с контекстом.
"""

import structlog
import sys
import logging
from typing import Any, Dict
from .config import config

def add_service_context(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Добавляет контекст сервиса к логам."""
    event_dict["service"] = config.service_name
    event_dict["environment"] = config.environment
    return event_dict

def add_timestamp(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Добавляет временную метку к логам."""
    import datetime
    event_dict["timestamp"] = datetime.datetime.utcnow().isoformat() + "Z"
    return event_dict

structlog.configure(
    processors=[
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        add_service_context,
        add_timestamp,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Настройка root logger для перехвата стандартных логов
numeric_level = getattr(logging, config.log_level.upper(), logging.INFO)
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=numeric_level,
)