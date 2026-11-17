-- catalog-override: engine-only: DuckDB reads Parquet through its read_parquet table function.
INSERT INTO {table} SELECT * FROM read_parquet('{path}')
