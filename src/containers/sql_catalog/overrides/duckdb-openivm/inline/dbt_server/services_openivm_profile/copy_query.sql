-- catalog-override: engine-only: DuckDB OpenIVM profiling exports query results through DuckDB COPY syntax.
COPY ({query}) TO {output_path} (HEADER, DELIMITER ',');
