FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dbt-projects/ /app/dbt-projects/
COPY app.py /app/app.py

# Install dbt deps for each project at build time
RUN cd /app/dbt-projects/spark && dbt deps --profiles-dir . 2>/dev/null || true

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "7200", "--workers", "2", "app:app"]
