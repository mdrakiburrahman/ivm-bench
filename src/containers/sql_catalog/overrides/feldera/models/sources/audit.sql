-- catalog-override: engine-only: Feldera-only ROW and scalar column schemas model its Delta/Parquet ingestion tables; no other engine has these models.
dataset VARCHAR,
batchid INTEGER,
"date" DATE,
attribute VARCHAR,
"value" BIGINT,
dvalue DECIMAL(15,5)
