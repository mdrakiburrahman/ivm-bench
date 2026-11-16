-- catalog-override: engine-only: DuckDB COPY exports the registered time-series DataFrame as Parquet
COPY ts_df TO '{path}' (FORMAT PARQUET)
