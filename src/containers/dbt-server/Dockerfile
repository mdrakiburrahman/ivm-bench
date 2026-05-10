FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the custom dbt-duckdb-openivm adapter
COPY adapters/dbt-duckdb-openivm /tmp/dbt-duckdb-openivm
RUN pip install --no-cache-dir /tmp/dbt-duckdb-openivm && rm -rf /tmp/dbt-duckdb-openivm

COPY dbt-projects/ /app/dbt-projects/
COPY handlers/ /app/handlers/
COPY services/ /app/services/
COPY models/ /app/models/
COPY app.py /app/app.py

# Install dbt deps for each project at build time
RUN cd /app/dbt-projects/spark && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/duckdb && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/feldera && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/duckdb-openivm && dbt deps --profiles-dir . 2>/dev/null || true

# Preinstall DuckDB extensions to avoid runtime download
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL ducklake; INSTALL sqlite'); c.close()"

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "604800", "--workers", "1", "app:app"]
