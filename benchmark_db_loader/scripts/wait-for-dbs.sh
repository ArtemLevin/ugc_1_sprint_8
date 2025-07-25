#!/bin/bash
# wait-for-dbs.sh

# set -e обеспечивает выход при любой ошибке
set -e

host="$1"
shift
cmd="$@"

# Цикл until повторяет проверку до успеха
until python -c "
# python -c вызывает Python код напрямую без создания временных файлов
import sys
# sys.path.append('/app') добавляет путь к модулям проекта
sys.path.append('/app')
from src.utils.wait_for_db import wait_for_all_databases
sys.exit(0 if wait_for_all_databases(max_attempts=30, delay=2) else 1)
"; do
  # Перенаправление в stderr для логов обеспечивает их видимость
  >&2 echo "Databases are not ready - sleeping"
  sleep 2
done

# Перенаправление в stderr для логов обеспечивает их видимость
>&2 echo "Databases are up - executing command"
# exec $cmd заменяет процесс скрипта на целевую команду (python src.main)
exec $cmd