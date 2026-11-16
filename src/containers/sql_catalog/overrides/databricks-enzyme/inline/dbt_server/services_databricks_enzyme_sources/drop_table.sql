-- catalog-override: engine-only: Databricks Enzyme replaces row-tracked managed Delta source tables on initialization.
DROP TABLE IF EXISTS {table}
