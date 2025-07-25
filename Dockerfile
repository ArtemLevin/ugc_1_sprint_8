FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir --pre -r requirements.txt

COPY . .

RUN mkdir -p /var/log/cinema-analytics \
 && chmod +x scripts/*.sh

RUN useradd -m -u 1000 appuser
USER appuser

EXPOSE 8000

ENTRYPOINT ["/app/scripts/start_api.sh"]