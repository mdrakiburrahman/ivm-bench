# Build revision-matched loadable ICU and JSON extensions without adding them to the
# static extension registry of OpenIVM's benchmark helper executables.
duckdb_extension_load(icu DONT_LINK)
duckdb_extension_load(json DONT_LINK)
