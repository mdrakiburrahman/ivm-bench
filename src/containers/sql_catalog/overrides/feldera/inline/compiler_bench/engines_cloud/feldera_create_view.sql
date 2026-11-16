-- catalog-override: engine-only: Feldera requires materialized views in its batch program so ad-hoc verification can read them.
CREATE MATERIALIZED VIEW {view_name} AS {sql};
