-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO DISTRICT SELECT w, d, 30000.00, 0.0500, 1, 'D'||d, 'Street1', 'Street2', 'City', 'ST', '123456789' FROM range(1, {n}) t(w), range(1, {d}) g(d)
