-- catalog-override: engine-only: DuckDB/OpenIVM's INSTALL, LOAD, SET, and PRAGMA lpts commands drive its compiler-only translation session.
PRAGMA lpts('SELECT * FROM ({sql}) AS __lpts_input');
