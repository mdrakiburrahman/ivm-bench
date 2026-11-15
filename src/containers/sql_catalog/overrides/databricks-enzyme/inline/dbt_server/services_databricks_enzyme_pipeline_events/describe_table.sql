-- catalog-override: engine-only: Databricks Enzyme reads Lakeflow pipeline IDs from Delta table properties via DESCRIBE EXTENDED.
DESCRIBE EXTENDED {table}
