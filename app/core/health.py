from flask import jsonify

from app.core.config import settings
from app.utils.cache import RedisPool
from app.core.logger import get_logger
from aiokafka import AIOKafkaProducer

logger = get_logger(__name__)


def register_health_check(app):
    @app.route("/health")
    async def health_check():
        redis_pool = RedisPool()
        try:
            client = await redis_pool.get_client()
            await client.ping()
            redis_status = {"status": "ok"}
        except Exception as e:
            logger.error("Redis health check failed", error=str(e))
            redis_status = {"status": "fail", "error": str(e)}

        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=settings.kafka.kafka_bootstrap_server
            )
            await producer.start()
            await producer.stop()
            kafka_status = {"status": "ok"}
        except Exception as e:
            logger.error("Kafka health check failed", error=str(e))
            kafka_status = {"status": "fail", "error": str(e)}

        # Определение общего статуса
        status_code = 200 if redis_status["status"] == "ok" and kafka_status["status"] == "ok" else 503

        return jsonify({
            "status": "healthy" if status_code == 200 else "unhealthy",
            "redis": redis_status,
            "kafka": kafka_status,
            "environment": settings.app.app_env,
            "service_name": settings.app.app_name
        }), status_code