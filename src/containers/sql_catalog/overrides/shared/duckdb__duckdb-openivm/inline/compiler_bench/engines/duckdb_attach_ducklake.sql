-- catalog-override: engine-only: DuckDB attaches DuckLake with extension-specific attachment options.
ATTACH IF NOT EXISTS '{meta_path}' AS dl (TYPE ducklake, DATA_PATH '{data_path}', data_inlining_row_limit 0)
