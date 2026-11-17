-- catalog-override: engine-only: Databricks enables row tracking and change data feed table properties required by Enzyme.
USING DELTA TBLPROPERTIES ('delta.enableRowTracking' = 'true', 'delta.enableChangeDataFeed' = 'true')
