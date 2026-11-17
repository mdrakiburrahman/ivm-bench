-- catalog-override: engine-only: DuckDB COPY exports the registered metrics DataFrame as Parquet
COPY long_df TO '{path}' (FORMAT PARQUET)
