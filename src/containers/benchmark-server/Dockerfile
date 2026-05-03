FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl jq ca-certificates gnupg pkg-config && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY handlers/ /app/handlers/
COPY services/ /app/services/
COPY models/ /app/models/
COPY app.py /app/app.py

EXPOSE 9000

CMD ["gunicorn", "--bind", "0.0.0.0:9000", "--timeout", "86400", "--workers", "1", "--threads", "8", "--preload", "app:app"]
