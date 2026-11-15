-- catalog-override: engine-only: Spark appends Delta change-feed properties to a dynamically loaded CREATE TABLE DDL fragment that SQLGlot cannot parse independently.
{stmt} USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
