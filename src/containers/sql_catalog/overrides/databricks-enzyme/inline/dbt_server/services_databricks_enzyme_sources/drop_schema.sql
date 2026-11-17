-- catalog-override: engine-only: Databricks Enzyme removes isolated Unity Catalog experiment schemas with CASCADE.
DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE
