-- catalog-override: engine-only: DuckDB metrics endpoint materializes read_parquet data into in-memory tables
CREATE TABLE {table} AS SELECT * FROM read_parquet(?)
