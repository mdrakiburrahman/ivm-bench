-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO CUSTOMER SELECT w, d, c, 0.05, 'GC', 'LastName', 'FirstName', 50000.00, 10000.00, 0.0, 0, 0, 'St1', 'St2', 'City', 'ST', '123456789', '1234567890123456', NOW(), 'M', 'data' FROM range(1, {n}) t(w), range(1, {d}) g(d), range(1, {c}) cc(c)
