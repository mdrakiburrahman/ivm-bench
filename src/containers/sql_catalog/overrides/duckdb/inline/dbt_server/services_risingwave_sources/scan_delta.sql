-- catalog-override: DuckDB delta_scan is the native table function used for local source reads.
SELECT {projection} FROM delta_scan({path})
