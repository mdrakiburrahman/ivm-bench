-- catalog-override: engine-only: DuckDB OpenIVM validation loads its extension and disables cascade refresh.
LOAD openivm;
SET openivm_cascade_refresh='off';
INSTALL icu; LOAD icu;
