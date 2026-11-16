-- catalog-override: engine-only: Databricks OpenIVM freezes recomputed model results in temporary Delta tables for validation.
CREATE TABLE {table} USING DELTA AS {query}
