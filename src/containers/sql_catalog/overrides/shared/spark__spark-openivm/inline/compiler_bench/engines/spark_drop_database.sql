-- catalog-override: engine-only: Spark removes the benchmark database and its tables with CASCADE.
DROP DATABASE IF EXISTS {schema} CASCADE
