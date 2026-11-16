-- catalog-override: engine-only: Databricks OpenIVM counts missing or extra rows with Delta EXCEPT ALL comparisons.
SELECT COUNT(*) FROM (({left}) EXCEPT ALL ({right}))
