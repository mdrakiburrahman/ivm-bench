-- catalog-override: engine-only: Feldera reads a materialized view when schema metadata is unavailable for verification.
SELECT * FROM {mv_name}
