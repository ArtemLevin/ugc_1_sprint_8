#!/bin/bash

# Создание основной структуры директорий
mkdir -p app/api
mkdir -p app/core
mkdir -p app/kafka
mkdir -p app/redis
mkdir -p app/clickhouse
mkdir -p app/etl
mkdir -p app/utils
mkdir -p scripts
mkdir -p tests

# Создание файлов в app/api/
touch app/api/__init__.py
touch app/api/routes.py
touch app/api/schemas.py

# Создание файлов в app/core/
touch app/core/__init__.py
touch app/core/config.py
touch app/core/logger.py
touch app/core/tracer.py

# Создание файлов в app/kafka/
touch app/kafka/__init__.py
touch app/kafka/producer.py
touch app/kafka/consumer.py

# Создание файлов в app/redis/
touch app/redis/__init__.py
touch app/redis/buffer.py
touch app/redis/dlq.py

# Создание файлов в app/clickhouse/
touch app/clickhouse/__init__.py
touch app/clickhouse/client.py
touch app/clickhouse/models.py

# Создание файлов в app/etl/
touch app/etl/__init__.py
touch app/etl/processor.py
touch app/etl/runner.py

# Создание файлов в app/utils/
touch app/utils/__init__.py
touch app/utils/validators.py

# Создание основных файлов в app/
touch app/main_api.py
touch app/main_etl.py

# Создание файлов в scripts/
touch scripts/start_api.sh
touch scripts/start_etl.sh
touch scripts/init_db.py
chmod +x scripts/start_api.sh
chmod +x scripts/start_etl.sh
chmod +x scripts/init_db.py

# Создание файлов в tests/
touch tests/__init__.py
touch tests/test_kafka_producer.py
touch tests/test_consumer.py
touch tests/test_etl.py

# Создание корневых файлов
touch docker-compose.yml
touch Dockerfile
touch requirements.txt
touch Makefile
touch README.md

echo "Структура проекта успешно создана!"
