FROM python:3.11-slim

# Установка netcat (для wait-for-it.sh)
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["gunicorn", "-b", "0.0.0.0:5000", "app.main:app"]