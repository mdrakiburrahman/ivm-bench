-- catalog-override: engine-only: Feldera-only ROW and scalar column schemas model its Delta/Parquet ingestion tables; no other engine has these models.
cdc_flag VARCHAR,
cdc_dsn BIGINT,
w_c_id BIGINT,
w_s_symb VARCHAR,
w_dts TIMESTAMP,
w_action VARCHAR
