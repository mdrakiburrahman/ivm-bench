-- catalog-override: engine-only: DuckDB COPY exports aggregated metrics as the public Parquet contract
COPY by_model_df TO '{path}' (FORMAT PARQUET)
