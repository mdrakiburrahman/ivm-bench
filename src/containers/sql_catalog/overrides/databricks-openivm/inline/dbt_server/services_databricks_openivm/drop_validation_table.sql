-- catalog-override: engine-only: Databricks OpenIVM cleans temporary Delta tables used by its result-validation path.
DROP TABLE IF EXISTS {table}
