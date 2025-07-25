import logging
import sys
from .config import LOG_LEVEL


def setup_logger(name: str) -> logging.Logger:
    """
    Настраивает и возвращает логгер с заданным именем.

    Args:
        name: Имя логгера

    Returns:
        Настроенный экземпляр logging.Logger
    """
    logger = logging.getLogger(name)

    # Преобразуем строку в уровень логирования
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    # Предотвращаем дублирование логов
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger