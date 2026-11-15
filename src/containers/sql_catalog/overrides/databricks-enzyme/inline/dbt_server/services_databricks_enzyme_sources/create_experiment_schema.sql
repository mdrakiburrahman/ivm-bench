-- catalog-override: engine-only: Databricks Enzyme isolates each benchmark run in Unity Catalog schemas.
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
