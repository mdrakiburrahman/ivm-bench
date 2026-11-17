-- catalog-override: engine-only: DuckDB metrics endpoint compares Parquet-backed rows with a FULL OUTER JOIN
SELECT {selects} FROM (SELECT * FROM metrics_by_model WHERE engine='spark') s FULL OUTER JOIN (SELECT * FROM metrics_by_model WHERE engine='spark-openivm') o ON s.batch = o.batch AND s.dbt_model = o.dbt_model{where} ORDER BY batch, dbt_model
