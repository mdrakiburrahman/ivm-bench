-- catalog-override: DuckDB builds the local Parquet scan from the runtime projection and source query.
SELECT {prefix}, source.* FROM ({scan_sql}) AS source
