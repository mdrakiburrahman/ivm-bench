-- catalog-override: engine-only: Databricks removes Enzyme materialized views and explain probes.
DROP MATERIALIZED VIEW IF EXISTS {mv_name}
