#!/bin/sh
# wait-for-it.sh - Скрипт для ожидания готовности сервисов перед запуском приложения

set -e  # Выход при ошибке

host="$1"
shift
cmd="$@"

# Настройки по умолчанию
TIMEOUT=60
INTERVAL=5
QUIET=0

# Функция вывода сообщений
log() {
  if [ "$QUIET" -ne 1 ]; then
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $*"
  fi
}

# Помощь
usage() {
  cat <<EOF
Использование: $0 HOST:PORT [-t SECS] [-- COMMAND ARGS...]
Ожидает, пока хост и порт станут доступны.

  -t SECS     Максимальное время ожидания (по умолчанию: $TIMEOUT)
  --          Команда для выполнения после успешного ожидания
  -q          Тихий режим (без логов)
  -h          Показать эту справку

Пример:
  $0 message-broker:9092 -t 30 -- python main.py
EOF
}

# Парсинг аргументов
while [ $# -gt 0 ]; do
  case "$1" in
    -t)
      TIMEOUT="$2"
      if ! echo "$TIMEOUT" | grep -E '^[0-9]+$' >/dev/null; then
        echo "Ошибка: таймаут должен быть числом"
        exit 1
      fi
      shift 2
      ;;
    -q)
      QUIET=1
      shift
      ;;
    --)
      shift
      cmd="$@"
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [ -z "$host" ]; then
        host="$1"
      fi
      shift
      ;;
  esac
done

# Разбор host:port
if ! echo "$host" | grep ':' >/dev/null; then
  log "Ошибка: адрес должен быть в формате HOST:PORT"
  exit 1
fi

HOST=$(echo "$host" | cut -d: -f1)
PORT=$(echo "$host" | cut -d: -f2)

# Проверка, что порт — число
if ! echo "$PORT" | grep -E '^[0-9]+$' >/dev/null; then
  log "Ошибка: порт должен быть числом"
  exit 1
fi

log "Ожидание $HOST:$PORT (таймаут: $TIMEOUT секунд)..."

elapsed=0
while ! nc -z "$HOST" "$PORT"; do
  elapsed=$((elapsed + INTERVAL))
  if [ "$elapsed" -gt "$TIMEOUT" ]; then
    log "Ошибка: таймаут ожидания $HOST:$PORT"
    exit 1
  fi
  log "Сервис $HOST:$PORT недоступен. Жду $INTERVAL сек..."
  sleep "$INTERVAL"
done

log "Сервис $HOST:$PORT готов!"

# Запуск команды
exec $cmd