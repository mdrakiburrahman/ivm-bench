-- catalog-override: engine-only: Databricks Serverless falls back to SHOW TABLES for fresh Unity Catalog schemas.
SHOW TABLES IN `{catalog}`.`{schema}`
