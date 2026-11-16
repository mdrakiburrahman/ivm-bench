-- catalog-override: engine-only: Databricks SQL reads Parquet from a Unity Catalog volume path.
INSERT INTO {table} SELECT * FROM parquet.`{path}`
