-- catalog-override: engine-only: Databricks Enzyme appends staged Delta data BY NAME to CDF-enabled source tables.
INSERT INTO {table} BY NAME SELECT * FROM delta.`{path}`
