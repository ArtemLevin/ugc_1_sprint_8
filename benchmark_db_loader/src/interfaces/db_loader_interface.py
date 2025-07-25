from abc import ABC, abstractmethod
import pandas as pd
from typing import Iterator


class DBLoaderInterface(ABC):
    """
    Интерфейс для загрузчиков данных в базы данных.

    Определяет контракт для всех реализаций загрузчиков,
    следуя принципу Dependency Inversion.
    """

    @abstractmethod
    def connect(self) -> None:
        """Устанавливает соединение с базой данных."""
        pass

    @abstractmethod
    def create_table(self) -> None:
        """Создает таблицу для бенчмарка."""
        pass

    @abstractmethod
    def load_data(self, data_batches: Iterator[pd.DataFrame]) -> float:
        """
        Загружает данные в базу данных.

        Args:
            data_batches: Итератор с батчами данных в формате DataFrame

        Returns:
            Время загрузки в секундах
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        pass

    @abstractmethod
    def log_start(self) -> None:
        """Логирует начало бенчмарка."""
        pass

    @abstractmethod
    def log_completion(self, elapsed: float) -> None:
        """
        Логирует завершение бенчмарка.

        Args:
            elapsed: Время выполнения в секундах
        """
        pass