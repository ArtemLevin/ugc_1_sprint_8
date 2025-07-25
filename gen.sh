#!/bin/bash

# Создание основной директории проекта
mkdir -p benchmark_db_loader

# Переход в директорию проекта
cd benchmark_db_loader

# Создание директорий
mkdir -p src/interfaces
mkdir -p src/databases
mkdir -p src/utils
mkdir -p scripts
mkdir -p results

# Создание файлов в корневой директории
touch .env
touch .env.example
touch docker-compose.yaml
touch Dockerfile
touch requirements.txt
touch README.md

# Создание файлов в src/
touch src/__init__.py
touch src/main.py
touch src/config.py
touch src/benchmark_runner.py
touch src/logging_config.py
touch src/di_container.py

# Создание файлов в src/interfaces/
touch src/interfaces/__init__.py
touch src/interfaces/db_loader_interface.py
touch src/interfaces/logger_interface.py

# Создание файлов в src/databases/
touch src/databases/__init__.py
touch src/databases/base_loader.py
touch src/databases/clickhouse_loader.py
touch src/databases/postgres_loader.py
touch src/databases/vertica_loader.py

# Создание файлов в src/utils/
touch src/utils/__init__.py
touch src/utils/data_generator.py
touch src/utils/wait_for_db.py
touch src/utils/results_saver.py

# Создание файлов в scripts/
touch scripts/wait-for-dbs.sh
chmod +x scripts/wait-for-dbs.sh

echo "Структура проекта benchmark_db_loader успешно создана!"
