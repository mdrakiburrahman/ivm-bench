-- catalog-override: engine-only: Databricks Enzyme removes its planner sandbox with Unity Catalog CASCADE semantics.
DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE
