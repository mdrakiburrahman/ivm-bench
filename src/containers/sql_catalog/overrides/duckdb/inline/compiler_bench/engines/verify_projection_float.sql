-- catalog-override: DuckDB verifier relies on the Spark DOUBLE rounding expression, which SQLGlot renders differently for the DuckDB dialect.
round(CAST({column} AS DOUBLE), 10) AS {column}
