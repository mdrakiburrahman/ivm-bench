-- catalog-override: engine-only: DuckDB OpenIVM profiling queries its extension-owned refresh profile table.
SELECT {exported_columns}, p.* REPLACE (COALESCE(v.view_sql_name, p.view_name) AS view_name)
FROM openivm_refresh_profile p
LEFT JOIN openivm_views v ON v.view_name = p.view_name
ORDER BY profile_timestamp, refresh_id, step_order
