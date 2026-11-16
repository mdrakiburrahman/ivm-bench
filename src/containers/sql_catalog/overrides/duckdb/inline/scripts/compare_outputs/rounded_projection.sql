-- catalog-override: engine-only: DuckDB cross-engine comparison preserves the Arrow column alias after rounding.
round("{column}", {precision}) AS "{column}"
