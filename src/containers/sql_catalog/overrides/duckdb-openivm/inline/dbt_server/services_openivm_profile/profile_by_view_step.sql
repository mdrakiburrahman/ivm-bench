-- catalog-override: engine-only: DuckDB OpenIVM profiling aggregates its extension-owned refresh profile table by view and step.
SELECT {exported_columns},
       COALESCE(v.view_sql_name, p.view_name) AS view_name,
       step_name,
       COUNT(*) AS row_count,
       SUM(duration_ms) AS total_ms,
       AVG(duration_ms) AS avg_ms,
       MAX(duration_ms) AS max_ms
FROM openivm_refresh_profile p
LEFT JOIN openivm_views v ON v.view_name = p.view_name
GROUP BY COALESCE(v.view_sql_name, p.view_name), step_name
ORDER BY total_ms DESC
