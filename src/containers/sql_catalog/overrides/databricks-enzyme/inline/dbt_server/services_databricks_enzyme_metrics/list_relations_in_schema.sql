-- catalog-override: engine-only: Databricks Enzyme discovers relation types from Unity Catalog information_schema.
SELECT table_schema, table_name, table_type FROM `{catalog}`.information_schema.tables WHERE table_schema = '{schema}'
