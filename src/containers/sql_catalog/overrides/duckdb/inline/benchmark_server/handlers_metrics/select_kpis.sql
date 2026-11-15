-- catalog-override: engine-only: DuckDB metrics endpoint queries its in-memory Parquet-derived table
SELECT * FROM metrics_by_model{where} ORDER BY engine, batch, dbt_model
