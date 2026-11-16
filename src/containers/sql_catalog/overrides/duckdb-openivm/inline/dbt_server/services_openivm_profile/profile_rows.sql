-- catalog-override: engine-only: DuckDB OpenIVM profiling queries its extension-owned refresh profile table.
SELECT {exported_columns}, *
FROM openivm_refresh_profile
ORDER BY profile_timestamp, refresh_id, step_order