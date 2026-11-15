-- catalog-override: DuckDB infers local scan columns by executing the query with LIMIT 0.
SELECT * FROM ({scan_sql}) AS scan LIMIT 0
