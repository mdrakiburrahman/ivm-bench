-- catalog-override: engine-only: DuckDB implements the baseline materialization as CREATE OR REPLACE TABLE.
CREATE OR REPLACE TABLE {mv_name} AS {sql}
