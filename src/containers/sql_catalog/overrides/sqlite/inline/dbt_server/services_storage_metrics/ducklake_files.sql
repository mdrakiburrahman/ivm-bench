-- catalog-override: engine-only: SQLite reads DuckLake metadata and file sizes from its catalog tables.

        SELECT s.schema_name, s.path, s.path_is_relative,
               t.table_name, t.path, t.path_is_relative,
               f.path, f.path_is_relative, f.file_size_bytes, f.file_kind
        FROM ducklake_schema s
        JOIN ducklake_table t USING (schema_id)
        JOIN (
            SELECT table_id, path, path_is_relative, file_size_bytes, 'data' AS file_kind
            FROM ducklake_data_file
            UNION ALL
            SELECT table_id, path, path_is_relative, file_size_bytes, 'delete' AS file_kind
            FROM ducklake_delete_file
        ) f USING (table_id)
