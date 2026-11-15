-- catalog-override: engine-only: DuckDB drops its baseline materialization as a table.
DROP TABLE IF EXISTS {mv_name}
