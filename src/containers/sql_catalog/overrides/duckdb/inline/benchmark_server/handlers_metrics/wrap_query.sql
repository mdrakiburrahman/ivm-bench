-- catalog-override: engine-only: DuckDB metrics route wraps caller-supplied SQL in a row-limited subquery
SELECT * FROM ({query}) AS _q LIMIT {cap}
