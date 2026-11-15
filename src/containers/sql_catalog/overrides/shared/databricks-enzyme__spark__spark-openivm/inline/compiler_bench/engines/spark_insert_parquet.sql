-- catalog-override: engine-only: Spark reads a Parquet data source using its parquet path relation syntax.
INSERT INTO {table} SELECT * FROM parquet.`{path}`
