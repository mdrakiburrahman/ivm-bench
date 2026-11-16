-- catalog-override: engine-only: DuckDB aggregates its registered metrics DataFrame using DuckDB SQL
SELECT engine, sf, run_id, batch, dbt_model, {aggregates} FROM long_df GROUP BY engine, sf, run_id, batch, dbt_model
