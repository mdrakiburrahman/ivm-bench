-- catalog-override: engine-only: DuckDB OpenIVM drops its extension-backed materialized view.
DROP MATERIALIZED VIEW IF EXISTS {mv_name}
