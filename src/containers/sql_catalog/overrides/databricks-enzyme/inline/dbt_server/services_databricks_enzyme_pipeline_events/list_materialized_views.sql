-- catalog-override: engine-only: Databricks Enzyme resolves MV backing pipelines from Unity Catalog metadata with a DB-API parameter.
SELECT table_name FROM {catalog}.information_schema.tables WHERE table_schema = ? AND table_type = 'MATERIALIZED_VIEW'
