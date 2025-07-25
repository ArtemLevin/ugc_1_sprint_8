#!/bin/bash

set -e

echo "Starting Cinema Analytics ETL Process..."

# Проверяем зависимости
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    exit 1
fi

# Активируем виртуальное окружение если оно есть
if [ -d "venv" ]; then
    source venv/bin/activate
fi

if [ -f "requirements.txt" ]; then
    echo "Installing dependencies..."
    pip install --no-cache-dir -r requirements.txt
fi

echo "Starting ETL process..."
python -m app.main_etl