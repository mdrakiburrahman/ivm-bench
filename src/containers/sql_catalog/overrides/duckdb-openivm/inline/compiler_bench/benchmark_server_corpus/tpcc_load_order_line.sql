-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO ORDER_LINE SELECT w, d, o, ol, (ol % 10) + 1, NULL, (10 + ol * 5) + 0.00, w, 5.00, 'DistInfo' FROM range(1, {n}) t(w), range(1, {d}) g(d), range(1, {o}) oo(o), range(1, {ol}) l(ol)
