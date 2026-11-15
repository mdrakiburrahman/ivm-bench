-- catalog-override: engine-only: DuckDB compares registered Arrow projections using EXCEPT ALL.
SELECT count(*) FROM (({left}) EXCEPT ALL ({right}))
