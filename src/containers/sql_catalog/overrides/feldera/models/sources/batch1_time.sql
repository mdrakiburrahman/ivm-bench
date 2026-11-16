-- catalog-override: engine-only: Feldera-only ROW and scalar column schemas model its Delta/Parquet ingestion tables; no other engine has these models.
sk_timeid BIGINT,
timevalue VARCHAR,
hourid INTEGER,
hourdesc VARCHAR,
minuteid INTEGER,
minutedesc VARCHAR,
secondid INTEGER,
seconddesc VARCHAR,
markethoursflag BOOLEAN,
officehoursflag BOOLEAN
