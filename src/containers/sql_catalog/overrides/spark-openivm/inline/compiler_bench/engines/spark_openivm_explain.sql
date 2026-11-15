-- catalog-override: engine-only: Spark OpenIVM classifies views with its extension-specific EXPLAIN CREATE MATERIALIZED VIEW command.
EXPLAIN CREATE MATERIALIZED VIEW {mv_name}_explain AS ({sql})
