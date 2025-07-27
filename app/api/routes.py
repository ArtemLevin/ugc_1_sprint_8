"""
API маршруты для приема пользовательских событий.

Обеспечивают валидацию данных и отправку в Kafka.
"""

from quart import Blueprint, request, jsonify, current_app
from pydantic import ValidationError
from ..core.logger import logger
from ..api.schemas import UserEventSchema, HealthResponseSchema, DLQStatusSchema
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

bp = Blueprint("api", __name__)


@bp.route("/health", methods=["GET"])
async def health_check():
    """
    Проверка состояния сервиса.

    Returns:
        JSON ответ с состоянием сервиса
    """
    with tracer.start_as_current_span("health_check"):
        response = HealthResponseSchema(
            status="ok",
            service="cinema-analytics-api",
            version="1.0.0"
        )
        return jsonify(response.model_dump()), 200

@bp.route("/event", methods=["POST"])
async def send_event():
    """
    Отправка пользовательского события.

    Принимает JSON с данными события, валидирует их и отправляет в Kafka.

    Примеры валидных запросов:
        curl -X POST http://localhost:8000/event -H "Content-Type: application/json" -d '{
            "user_id": "user123",
            "event_id": "event123",
            "timestamp": "2025-07-26T09:20:46Z",
            "event_type": "click",
            "target": "movie_card"
        }'

        curl -X POST http://localhost:8000/event -H "Content-Type: application/json" -d '{
            "user_id": "user123",
            "event_id": "event123",
            "timestamp": "2025-07-26T09:20:46Z",
            "event_type": "page_view",
            "page": "asda",
            "duration_seconds": "10"
        }'

        curl -X POST http://localhost:8000/event -H "Content-Type: application/json" -d '{
            "user_id": "user123",
            "event_id": "event123",
            "timestamp": "2025-07-26T09:20:46Z",
            "event_type": "quality_change",
            "from_quality": "720",
            "to_quality": "1480",
            "movie_id": "123"
        }'

        curl -X POST http://localhost:8000/event -H "Content-Type: application/json" -d '{
            "user_id": "user123",
            "event_id": "event123",
            "timestamp": "2025-07-26T09:20:46Z",
            "event_type": "watched_to_end",
            "movie_id": "123"
        }'

        curl -X POST http://localhost:8000/event -H "Content-Type: application/json" -d '{
            "user_id": "user123",
            "event_id": "event123",
            "timestamp": "2025-07-26T09:20:46Z",
            "event_type": "filter_used",
            "filter_name": "genre",
            "filter_value": "comedy"
        }'

    Returns:
        JSON ответ с результатом операции
    """
    with tracer.start_as_current_span("send_event") as span:
        try:
            data = await request.get_json()

            if not data:
                span.set_status(trace.Status(trace.StatusCode.ERROR, "No JSON data"))
                return jsonify({"error": "No JSON data provided"}), 400

            span.set_attribute("event.user_id", data.get("user_id", "unknown"))
            span.set_attribute("event.movie_id", data.get("movie_id", "unknown"))
            span.set_attribute("event.event_type", data.get("event_type", "unknown"))

            try:
                event = UserEventSchema.validate_python(data)
                validated_data = event.model_dump()
                logger.info("Event validated successfully", validated_event=validated_data) # ругалось на повторение event
            except ValidationError as e:
                span.set_status(trace.Status(trace.StatusCode.ERROR, "Validation failed"))
                logger.error("Event validation failed", errors=e.errors())
                return jsonify({
                    "error": "Validation failed",
                    "details": e.errors()
                }), 400

            producer = current_app.extensions['kafka_producer']
            await producer.send_event(validated_data)

            span.set_status(trace.Status(trace.StatusCode.OK))
            logger.info("Event processed successfully", validated_event=validated_data) # ругалось на повторение event

            return jsonify({"status": "ok", "message": "Event sent successfully"}), 200

        except Exception as e:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            logger.error("Failed to process event", error=str(e), exc_info=True)
            return jsonify({"error": "Internal server error"}), 500



@bp.route("/dlq", methods=["GET"])
async def get_dlq_status():
    """
    Получение статуса Dead Letter Queue.

    Returns:
        JSON ответ со статусом DLQ
    """
    # TODO: Реализовать получение статуса DLQ
    with tracer.start_as_current_span("get_dlq_status"):
        response = DLQStatusSchema(
            dlq_length=0,
            dlq_enabled=True
        )
        return jsonify(response.model_dump()), 200