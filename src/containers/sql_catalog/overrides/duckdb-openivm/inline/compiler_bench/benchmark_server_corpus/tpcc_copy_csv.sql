-- catalog-override: engine-only: DuckDB's COPY TO syntax exports the shared TPC-C Parquet and CSV seed files.
COPY {table} TO '{target}' (FORMAT CSV, HEADER FALSE);
