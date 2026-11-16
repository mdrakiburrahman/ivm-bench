-- catalog-override: engine-only: DuckDB reads A/B event rows from Parquet for the selected models
SELECT engine, dbt_model, metric_name, event_ts, metric_value FROM {rp} WHERE batch={batch} AND event_ts IS NOT NULL AND dbt_model IN ({model_list})
