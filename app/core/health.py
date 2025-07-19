from flask import jsonify
from aiokafka import AIOKafkaProducer
from app.utils.cache import RedisPool
from app.core.logger import get_logger

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
            redis_status = {"status": "fail", "error": str(e)}

        try:
            producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
            await producer.start()
            await producer.stop()
            kafka_status = {"status": "ok"}
        except Exception as e:
            kafka_status = {"status": "fail", "error": str(e)}

        status_code = 200 if redis_status["status"] == "ok" and kafka_status["status"] == "ok" else 503
        return jsonify({
            "status": "healthy" if status_code == 200 else "unhealthy",
            "redis": redis_status,
            "kafka": kafka_status
        }), status_code