-- catalog-override: engine-only: DuckDB DESCRIBE SELECT inspects registered Arrow tables during result comparison.
DESCRIBE SELECT * FROM {relation}
