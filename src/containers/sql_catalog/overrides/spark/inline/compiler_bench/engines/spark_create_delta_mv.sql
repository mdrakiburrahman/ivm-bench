-- catalog-override: engine-only: Spark implements baseline materialization as a Delta table rather than a materialized view.
CREATE OR REPLACE TABLE {mv_name} USING DELTA AS {sql}
