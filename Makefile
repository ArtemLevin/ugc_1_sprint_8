.PHONY: help up down logs test api etl init-db clean

GREEN := \033[0;32m
BLUE := \033[0;34m
RED := \033[0;31m
NC := \033[0m # No Color

help:
    @echo "$(BLUE)Доступные команды:$(NC)"
    @echo "  $(GREEN)make up$(NC)         - Запустить все сервисы"
    @echo "  $(GREEN)make down$(NC)       - Остановить все сервисы"
    @echo "  $(GREEN)make logs$(NC)       - Показать логи всех сервисов"
    @echo "  $(GREEN)make logs-api$(NC)   - Показать логи API сервиса"
    @echo "  $(GREEN)make logs-etl$(NC)   - Показать логи ETL процесса"
    @echo "  $(GREEN)make test$(NC)       - Запустить тесты"
    @echo "  $(GREEN)make api$(NC)        - Отправить тестовое событие"
    @echo "  $(GREEN)make init-db$(NC)    - Инициализировать базу данных"
    @echo "  $(GREEN)make clean$(NC)      - Очистить все данные"

up:
    @echo "$(BLUE)Запуск всех сервисов...$(NC)"
    docker-compose up -d
    @echo "$(GREEN)Сервисы запущены!$(NC)"

down:
    @echo "$(BLUE)Остановка всех сервисов...$(NC)"
    docker-compose down
    @echo "$(GREEN)Сервисы остановлены!$(NC)"

logs:
    @echo "$(BLUE)Показ логов всех сервисов...$(NC)"
    docker-compose logs -f

logs-api:
    @echo "$(BLUE)Показ логов API сервиса...$(NC)"
    docker-compose logs -f api

logs-etl:
    @echo "$(BLUE)Показ логов ETL процесса...$(NC)"
    docker-compose logs -f etl

test:
    @echo "$(BLUE)Запуск тестов...$(NC)"
    docker-compose exec api pytest

api:
    @echo "$(BLUE)Отправка тестового события...$(NC)"
    curl -X POST http://localhost:8000/event \
        -H "Content-Type: application/json" \
        -d '{"user_id": "test_user", "movie_id": "test_movie", "event_type": "start", "timestamp": "2024-01-01T12:00:00"}'

init-db:
    @echo "$(BLUE)Инициализация базы данных...$(NC)"
    docker-compose exec clickhouse clickhouse-client -q "CREATE DATABASE IF NOT EXISTS default"
    docker-compose exec api python scripts/init_db.py

clean:
    @echo "$(RED)Очистка всех данных...$(NC)"
    docker-compose down -v
    @echo "$(GREEN)Все данные очищены!$(NC)"