"""Read an engine's gold Delta output as its CURRENT logical contents.

Shared by compare_outputs.py and diff_columns.py. A gold table's raw parquet files are
not its contents, for three reasons. Each one produced false diffs before this module
existed:

* DbspNet's incremental output retracts rows with Delta **deletion vectors**. A reader
  that ignores them sees every retracted row as live: market_volatility looked like
  3,678 rows instead of 1,796.
* Feldera's output is a **change log**. Every row carries ``__feldera_op`` (``'i'``
  insert or ``'d'`` delete) and ``__feldera_ts``. Read raw, the ``'d'`` rows look like
  duplicates: market_volatility looked like 5,550 rows, and dim_trade / fact_watches
  looked like "SCD tie" extras. We net each distinct row's inserts against its deletes
  and drop both columns.
* Feldera writes TIMESTAMP as Delta ``timestamp`` (UTC-adjusted). DbspNet writes
  ``timestamp_ntz``. With the session TimeZone at UTC, casting the first to TIMESTAMP
  gives the same instant on both sides, so the two compare equal.

``delta_scan`` (DuckDB's delta extension, delta-kernel-rs) reads both engines' tables
and applies the deletion vectors. The extension is fetched on first use, so a run needs
network access once.
"""
import os

import duckdb


def connect():
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    return con


def register(con, base, engine, view):
    """Register ``<engine>_<view>`` as a temp view of the table's live rows, or return None
    when the table does not exist."""
    path = os.path.join(base, engine, "gold", view)
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        return None
    name = f"{engine}_{view}"
    raw = f"delta_scan('{path}')"
    desc = con.execute(f"DESCRIBE SELECT * FROM {raw}").fetchall()
    cols = []
    for c, t, *_ in desc:
        if c.startswith("__feldera_"):
            continue
        if t.upper() == "TIMESTAMP WITH TIME ZONE":
            cols.append(f'CAST("{c}" AS TIMESTAMP) AS "{c}"')
        else:
            cols.append(f'"{c}"')
    sel = ", ".join(cols)

    if not any(c == "__feldera_op" for c, *_ in desc):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT {sel} FROM {raw}")
        return name

    # Net the change log: weight +1 per insert, -1 per delete, per distinct row. Then
    # repeat each surviving row by its weight, so a bag with duplicates stays a bag.
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {name}__net AS "
        f"SELECT {sel}, SUM(CASE __feldera_op WHEN 'd' THEN -1 ELSE 1 END)::BIGINT AS __w "
        f"FROM {raw} GROUP BY ALL HAVING __w <> 0")
    negative = con.execute(f"SELECT count(*) FROM {name}__net WHERE __w < 0").fetchone()[0]
    if negative:
        raise ValueError(f"{engine}/{view}: {negative} rows deleted more often than inserted")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {name} AS "
        f"SELECT n.* EXCLUDE (__w) FROM {name}__net n, range(n.__w)")
    return name
