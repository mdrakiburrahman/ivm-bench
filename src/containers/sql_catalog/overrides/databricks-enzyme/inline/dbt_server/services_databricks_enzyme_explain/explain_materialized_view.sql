-- catalog-override: engine-only: Databricks Enzyme exposes incremental materialized-view planning through EXPLAIN.
EXPLAIN CREATE MATERIALIZED VIEW {target} REFRESH POLICY INCREMENTAL STRICT AS
{compiled_sql}
