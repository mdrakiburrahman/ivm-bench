-- catalog-override: engine-only: Databricks OpenIVM materializes physical model state as CDF-enabled Delta tables.
CREATE TABLE {table} USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS {query}
