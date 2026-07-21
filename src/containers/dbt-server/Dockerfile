FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl git ca-certificates gnupg lsb-release && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/apt/keyrings && \
    curl -sLS https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    chmod go+r /etc/apt/keyrings/microsoft.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" \
        > /etc/apt/sources.list.d/azure-cli.list && \
    apt-get update && apt-get install -y --no-install-recommends azure-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# azcopy — stages raw Delta source tables into the OneLake Files cache.
RUN curl -sL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy.tgz && \
    tar -xzf /tmp/azcopy.tgz -C /tmp && \
    cp /tmp/azcopy_linux_*/azcopy /usr/local/bin/azcopy && \
    chmod +x /usr/local/bin/azcopy && \
    rm -rf /tmp/azcopy*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install the custom dbt-duckdb-openivm adapter
COPY adapters/dbt-duckdb-openivm /tmp/dbt-duckdb-openivm
RUN pip install --no-cache-dir /tmp/dbt-duckdb-openivm && rm -rf /tmp/dbt-duckdb-openivm

# dbt-fabricspark's CLI auth constructs AzureCliCredential() with the default
# 10s subprocess timeout. Under a 30-thread build the token-refresh storm in the
# token's last 5 min spawns many concurrent `az` calls; occasional host CPU
# contention or relay latency pushes one past 10s → "Failed to invoke the Azure
# CLI" fails a node. Bump it to 60s. The grep guards against a silent
# no-op if a future dbt-fabricspark version changes the call site.
RUN LIVY=$(python -c "import dbt.adapters.fabricspark as m,os;print(os.path.join(os.path.dirname(m.__file__),'livysession.py'))") && \
    sed -i 's/AzureCliCredential()\.get_token/AzureCliCredential(process_timeout=60).get_token/g' "$LIVY" && \
    grep -q "process_timeout=60" "$LIVY"

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
RUN cd /app/dbt-projects/spark-openivm && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/databricks-enzyme && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/fabric-openivm-jvm-35 && dbt deps --profiles-dir . 2>/dev/null || true
RUN cd /app/dbt-projects/fabric-jvm-35 && dbt deps --profiles-dir . 2>/dev/null || true

# Preinstall DuckDB extensions to avoid runtime download
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL ducklake; INSTALL sqlite'); c.close()"

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--timeout", "604800", "--workers", "1", "app:app"]
