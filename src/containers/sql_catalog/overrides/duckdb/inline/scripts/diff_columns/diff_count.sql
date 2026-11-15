-- catalog-override: engine-only: DuckDB compares individual Arrow columns using EXCEPT ALL.
SELECT count(*) FROM (SELECT {expression} AS x FROM {left} EXCEPT ALL SELECT {expression} AS x FROM {right})
