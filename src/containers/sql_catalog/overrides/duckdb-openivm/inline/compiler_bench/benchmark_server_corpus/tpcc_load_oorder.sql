-- catalog-override: engine-only: DuckDB's range() table function and || concatenation are required by the native TPC-C seed statements.
INSERT INTO OORDER SELECT w, d, o, (o % {c}) + 1, NULL, 5, 1, NOW() FROM range(1, {n}) t(w), range(1, {d}) g(d), range(1, {o}) oo(o)
