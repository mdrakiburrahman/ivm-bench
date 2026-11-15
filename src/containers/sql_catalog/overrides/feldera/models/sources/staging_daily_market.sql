-- catalog-override: engine-only: Feldera-only ROW and scalar column schemas model its Delta/Parquet ingestion tables; no other engine has these models.
cdc_flag VARCHAR,
cdc_dsn BIGINT,
dm_date DATE,
dm_s_symb VARCHAR,
dm_close DOUBLE,
dm_high DOUBLE,
dm_low DOUBLE,
dm_vol INTEGER
