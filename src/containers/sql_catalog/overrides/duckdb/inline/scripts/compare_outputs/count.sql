-- catalog-override: engine-only: DuckDB counts rows in registered Arrow tables for cross-engine result comparison.
SELECT count(*) FROM {relation}
