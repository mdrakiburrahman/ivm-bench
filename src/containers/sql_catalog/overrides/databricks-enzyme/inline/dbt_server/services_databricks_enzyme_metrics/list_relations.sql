-- catalog-override: engine-only: Databricks Enzyme enumerates experiment relations through Unity Catalog information_schema.
SELECT table_schema, table_name, table_type FROM `{catalog}`.information_schema.tables WHERE table_schema IN ({schemas})
