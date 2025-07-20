"""
Модуль проверки работоспособности сервиса.

Содержит функции для регистрации и реализации health check-эндпоинта.
"""

from flask import Flask, jsonify
from typing import Tuple, Dict, Any, Callable, Awaitable
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.utils.cache import RedisService
import logging

logger = logging.getLogger(__name__)

HealthCheck = Callable[[], Awaitable[Tuple[bool, str]]]


class HealthCheckService:
    """
    Сервис для проверки здоровья компонентов приложения.

    Attributes:
        redis_service: Сервис для работы с Redis.
    """

    def __init__(self, redis_service: RedisService):
        self.redis_service = redis_service

    async def check_redis(self) -> Tuple[bool, str]:
        """
        Проверяет работоспособность Redis.

        Returns:
            Кортеж из флага успешности проверки и сообщения.
        """
        try:
            client = await self.redis_service.get_client()
            pong = await client.ping()
            if pong == b'PONG':
                logger.info("Redis health check passed")
                return True, "Redis is operational"
            logger.warning("Redis health check failed", reason="no pong response")
            return False, "Redis ping failed"
        except Exception as e:
            logger.error("Redis health check failed", exc_info=True, error=str(e))
            return False, f"Redis connection error: {str(e)}"

    async def check_kafka(self) -> Tuple[bool, str]:
        """
        Проверяет работоспособность Kafka.

        Returns:
            Кортеж из флага успешности проверки и сообщения.
        """
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka.bootstrap_server
            )
            await producer.start()
            await producer.stop()
            logger.info("Kafka health check passed")
            return True, "Kafka is operational"
        except Exception as e:
            logger.error("Kafka health check failed", exc_info=True, error=str(e))
            return False, f"Kafka connection error: {str(e)}"

    async def check_database(self) -> Tuple[bool, str]:
        """
        Проверяет работоспособность базы данных.
        В данном случае не реализовано, но может быть расширено.

        Returns:
            Кортеж из флага успешности проверки и сообщения.
        """
        logger.warning("Database health check not implemented")
        return True, "Database check not implemented"

    async def get_health_status(self) -> Tuple[Dict[str, Any], int]:
        """
        Выполняет все проверки и возвращает общий статус приложения.

        Returns:
            Кортеж из словаря с деталями статуса и HTTP-статус-кода.
        """
        checks = {
            "redis": self.check_redis,
            "kafka": self.check_kafka,
            "database": self.check_database
        }

        results = {}
        all_ok = True

        for name, check in checks.items():
            ok, message = await check()
            results[name] = {"status": "ok" if ok else "fail", "message": message}
            if not ok:
                all_ok = False

        status_code = 200 if all_ok else 503
        return {
            "status": "healthy" if status_code == 200 else "unhealthy",
            **results,
            "environment": settings.app.environment,
            "service_name": settings.app.name
        }, status_code


def register_health_check(app: Flask) -> None:
    """
    Регистрирует эндпоинт /health в Flask-приложении.

    Args:
        app: Flask-приложение, в которое нужно зарегистрировать эндпоинт.
    """
    health_check_service = HealthCheckService(RedisService())

    @app.route("/health")
    async def health_check():
        """
        Эндпоинт проверки работоспособности сервиса.

        Returns:
            JSON-ответ с деталями статуса и соответствующим HTTP-статус-кодом.
        """
        status_details, status_code = await health_check_service.get_health_status()
        return jsonify(status_details), status_code