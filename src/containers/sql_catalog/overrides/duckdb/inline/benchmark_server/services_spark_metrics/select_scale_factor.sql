-- catalog-override: engine-only: DuckDB reads the scale-factor metadata from the metrics Parquet file
SELECT sf FROM {rp} LIMIT 1
