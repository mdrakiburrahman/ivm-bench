-- catalog-override: engine-only: Spark OpenIVM drops an extension-backed materialized view and its dependents.
DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE
