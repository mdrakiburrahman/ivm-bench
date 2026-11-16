-- catalog-override: engine-only: DuckDB reads available metric names from the Parquet event data
SELECT DISTINCT metric_name FROM {rp} WHERE batch={batch} AND event_ts IS NOT NULL
