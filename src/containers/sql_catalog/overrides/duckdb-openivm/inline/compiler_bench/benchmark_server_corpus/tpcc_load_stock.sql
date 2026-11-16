-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO STOCK SELECT w, i, 50 + (i % 50), 0.00, 0, 0, 'StockData', 'Dist1', 'Dist2', 'Dist3', 'Dist4', 'Dist5', 'Dist6', 'Dist7', 'Dist8', 'Dist9', 'Dist10' FROM range(1, {n}) t(w), range(1, {i}) g(i)
