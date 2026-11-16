-- catalog-override: engine-only: DuckDB OpenIVM attaches its own SQLite-backed DuckLake metadata and data directory.
ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake (DATA_PATH '{data_path}', data_inlining_row_limit 0);
