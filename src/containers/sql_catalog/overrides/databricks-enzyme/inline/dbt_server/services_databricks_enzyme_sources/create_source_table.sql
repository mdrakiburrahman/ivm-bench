-- catalog-override: engine-only: Databricks Enzyme CTAS enables row tracking and CDF on managed Delta sources.
CREATE TABLE {table} TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableChangeDataFeed' = 'true') AS SELECT * FROM delta.`{path}`
