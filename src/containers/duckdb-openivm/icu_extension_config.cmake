# Build a revision-matched loadable ICU extension. DuckDB 2.0 only links an
# extension statically when explicitly requested, so DONT_LINK is unnecessary.
duckdb_extension_load(icu)
