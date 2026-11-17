-- catalog-override: engine-only: Databricks Enzyme classifies strict incremental materialized views through its REFRESH POLICY syntax.
EXPLAIN CREATE MATERIALIZED VIEW {probe} REFRESH POLICY INCREMENTAL STRICT AS {sql}
