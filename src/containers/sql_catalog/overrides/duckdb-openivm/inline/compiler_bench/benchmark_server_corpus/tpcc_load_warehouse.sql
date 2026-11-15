-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO WAREHOUSE SELECT w, 300000.00, 0.0500, 'WH'||w, 'Street1', 'Street2', 'City', 'ST', '123456789' FROM range(1, {n}) t(w)
