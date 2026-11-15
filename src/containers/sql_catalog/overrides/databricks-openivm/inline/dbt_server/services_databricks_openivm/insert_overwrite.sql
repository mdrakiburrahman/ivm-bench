-- catalog-override: engine-only: Databricks OpenIVM uses Delta INSERT OVERWRITE for full model recomputations.
INSERT OVERWRITE {table} {query}
