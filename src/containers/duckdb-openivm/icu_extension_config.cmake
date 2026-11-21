# Build revision-matched ICU and JSON artifacts. DuckDB 2.0 separates loading
# from static linking; OpenIVM's extension config owns the static link set.
duckdb_extension_load(icu)
duckdb_extension_load(json)
