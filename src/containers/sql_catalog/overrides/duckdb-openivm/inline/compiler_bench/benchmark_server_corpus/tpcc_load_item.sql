-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO ITEM SELECT i, 'Item'||i, (10 + (i % 90)) + 0.99, 'ItemData', (i % 10) + 1 FROM range(1, {i}) t(i)
