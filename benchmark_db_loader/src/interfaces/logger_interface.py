from abc import ABC, abstractmethod
from typing import Any


class LoggerInterface(ABC):
    """
    Интерфейс для логгера.

    Позволяет абстрагироваться от конкретной реализации логирования,
    следуя принципу Dependency Inversion.
    """

    @abstractmethod
    def debug(self, message: str) -> None:
        """
        Логирует сообщение уровня DEBUG.

        Args:
            message: Сообщение для логирования
        """
        pass

    @abstractmethod
    def info(self, message: str) -> None:
        """
        Логирует сообщение уровня INFO.

        Args:
            message: Сообщение для логирования
        """
        pass

    @abstractmethod
    def warning(self, message: str) -> None:
        """
        Логирует сообщение уровня WARNING.

        Args:
            message: Сообщение для логирования
        """
        pass

    @abstractmethod
    def error(self, message: str) -> None:
        """
        Логирует сообщение уровня ERROR.

        Args:
            message: Сообщение для логирования
        """
        pass

    @abstractmethod
    def critical(self, message: str) -> None:
        """
        Логирует сообщение уровня CRITICAL.

        Args:
            message: Сообщение для логирования
        """
        pass