-- catalog-override: engine-only: Databricks OpenIVM exposes Delta Change Data Feed rows through worker-local temporary views.
CREATE OR REPLACE TEMP VIEW {view} AS {query}
