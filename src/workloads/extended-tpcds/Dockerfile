FROM python:3.11-slim
WORKDIR /workload
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "import duckdb; c=duckdb.connect(); c.execute(\"SET custom_extension_repository='https://extensions.duckdb.org'\"); c.execute('INSTALL tpcds')"
COPY . .
CMD ["python", "prepare_experiment.py"]
