import asyncio
from flask import jsonify
from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis
from app.core.config import settings
from app.core.logger import get_logger

logger = get_logger(__name__)

async def check_redis():
    redis = Redis.from_url(settings.redis_url)
    try:
        await asyncio.wait_for(redis.ping(), timeout=5)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "fail", "error": str(e)}

async def check_kafka():
    try:
        producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_server)
        await asyncio.wait_for(producer.start(), timeout=5)
        await asyncio.wait_for(producer.stop(), timeout=5)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "fail", "error": str(e)}

def register_health_check(app):
    @app.route("/health")
    async def health_check():
        redis_status = await check_redis()
        kafka_status = await check_kafka()
        status_code = 200 if redis_status["status"] == "ok" and kafka_status["status"] == "ok" else 503
        return {
            "status": "healthy" if status_code == 200 else "unhealthy",
            "redis": redis_status,
            "kafka": kafka_status
        }, status_code