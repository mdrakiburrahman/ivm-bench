-- catalog-override: engine-only: Feldera-only ROW and scalar column schemas model its Delta/Parquet ingestion tables; no other engine has these models.
cdc_flag VARCHAR,
cdc_dsn BIGINT,
ct_ca_id BIGINT,
ct_dts TIMESTAMP,
ct_amt DOUBLE,
ct_name VARCHAR
