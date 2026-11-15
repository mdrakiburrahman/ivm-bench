-- catalog-override: engine-only: SQLite reads DuckLake's optional inlined-data metadata.
SELECT table_id, table_name FROM ducklake_inlined_data_tables
