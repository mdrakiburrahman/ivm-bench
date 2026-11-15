-- catalog-override: engine-only: DuckDB casts Arrow column samples to VARCHAR and compares using EXCEPT ALL.
SELECT CAST({expression} AS VARCHAR) FROM {left} EXCEPT ALL SELECT CAST({expression} AS VARCHAR) FROM {right} LIMIT 4
