-- catalog-override: engine-only: SQLite reads table and schema paths from DuckLake catalog metadata.
SELECT t.table_id, s.schema_name, s.path, s.path_is_relative, t.table_name, t.path, t.path_is_relative FROM ducklake_table t JOIN ducklake_schema s USING (schema_id)
