-- catalog-override: DuckDB COPY accepts a query body that SQLGlot cannot parse when the query is a runtime parameter.
COPY ({scan_sql}) TO {destination} (FORMAT PARQUET, COMPRESSION SNAPPY, PER_THREAD_OUTPUT TRUE, ROW_GROUP_SIZE 122880)
