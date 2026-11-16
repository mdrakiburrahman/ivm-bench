-- catalog-override: engine-only: DuckDB refreshes OpenIVM views through its extension PRAGMA.
PRAGMA refresh('{mv_name}')
