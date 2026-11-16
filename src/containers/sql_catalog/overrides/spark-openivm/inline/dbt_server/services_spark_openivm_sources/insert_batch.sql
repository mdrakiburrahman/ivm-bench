-- catalog-override: engine-only: Spark OpenIVM appends Delta rows with DuckDB-style BY NAME column mapping.
INSERT INTO tpcdi.staging_{table_name} BY NAME SELECT * FROM delta.`{source_path}`