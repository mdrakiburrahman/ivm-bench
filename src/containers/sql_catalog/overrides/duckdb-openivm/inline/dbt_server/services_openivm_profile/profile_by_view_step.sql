-- catalog-override: engine-only: DuckDB OpenIVM profiling aggregates its extension-owned refresh profile table by view and step.
SELECT {exported_columns},
       view_name,
       step_name,
       COUNT(*) AS row_count,
       SUM(duration_ms) AS total_ms,
       AVG(duration_ms) AS avg_ms,
       MAX(duration_ms) AS max_ms
FROM openivm_refresh_profile
GROUP BY view_name, step_name
ORDER BY total_ms DESC