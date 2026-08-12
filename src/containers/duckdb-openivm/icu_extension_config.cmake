# Build a revision-matched loadable ICU extension without adding it to the
# static extension registry of OpenIVM's benchmark helper executables.
duckdb_extension_load(icu DONT_LINK)
