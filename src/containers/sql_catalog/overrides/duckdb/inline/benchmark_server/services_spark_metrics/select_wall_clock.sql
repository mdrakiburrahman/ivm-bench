-- catalog-override: engine-only: DuckDB aggregates execution durations from its registered DataFrame
SELECT engine, batch, dbt_model, COALESCE(SUM(duration_ms), 0) AS wall_clock_ms FROM exec_df GROUP BY engine, batch, dbt_model
