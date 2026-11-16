-- catalog-override: engine-only: Databricks OpenIVM removes a Delta scratch table after incremental refresh.
DROP TABLE IF EXISTS {table}
