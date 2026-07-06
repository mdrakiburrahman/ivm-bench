FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY imds_relay_router.py /app/imds_relay_router.py

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD curl -sf "http://localhost:${IMDS_ROUTER_PORT:-8080}/healthz" || exit 1

CMD ["python3", "/app/imds_relay_router.py"]
