-- catalog-override: engine-only: DuckDB information_schema supplies the metrics table's columns
SELECT column_name FROM information_schema.columns WHERE table_name='metrics_by_model' AND column_name NOT IN ('engine','sf','run_id','batch','dbt_model')
