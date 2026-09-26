#!/usr/bin/env python3
"""Per-column drill-down for views that compare_outputs.py flags as different.

For each view it registers both engines' current live parquet (via _delta_log replay,
read through pyarrow) and, for every column, compares the *multiset* of values on each
side — order-independent, no row alignment needed:

    diff(col) = |dbspnet.col EXCEPT ALL feldera.col| + |feldera.col EXCEPT ALL dbspnet.col|

diff==0 => that column's values match exactly (as a bag). diff>0 => it diverges, and a
few example values from each side are printed. This separates:
  * surrogate-key / timestamp-formatting artifacts (only sk_* or a formatted ts column
    diverges, everything else matches), from
  * real semantic differences (many data columns diverge, or row counts differ).

FLOAT/DOUBLE columns are rounded to FLOAT_ROUND (default 6). DECIMAL columns are compared
as-is, so a scale/precision mismatch shows up here (that's the point).

Run (from repo root, after a run with PRESERVE_RESULTS=1). VIEWS defaults to the views
compare_outputs.py flagged; override by passing names as args or via the VIEWS env:

    docker run --rm -w /work \
      -v "$PWD/mount:/mount:ro" \
      -v "$PWD/src/.scripts/diff_columns.py:/work/diff_columns.py:ro" \
      python:3.11-slim \
      bash -c "pip install --quiet duckdb 'pyarrow==17.0.0' 'numpy<2' && \
               SF=3 python3 diff_columns.py dim_account market_volatility trade_volume_stats \
                                            customer_concentration broker_performance dim_trade fact_watches"
"""
import glob
import json
import os
import sys
import urllib.parse

import duckdb
import pyarrow.parquet as pq

SF = os.environ.get("SF", "3")
BASE = os.environ.get("RESULTS_BASE", f"/mount/results/{SF}")
FLOAT_ROUND = int(os.environ.get("FLOAT_ROUND", "6"))

DEFAULT_VIEWS = [
    "dim_account", "dim_trade", "fact_watches", "trade_volume_stats",
    "market_volatility", "customer_concentration", "broker_performance",
]


def live_files(table_dir):
    log = os.path.join(table_dir, "_delta_log")
    live = {}
    for commit in sorted(glob.glob(os.path.join(log, "*.json"))):
        with open(commit) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                act = json.loads(line)
                if "add" in act:
                    live[act["add"]["path"]] = True
                elif "remove" in act:
                    live.pop(act["remove"]["path"], None)
    return [os.path.join(table_dir, urllib.parse.unquote(p)) for p in live]


def expr(col, typ):
    if FLOAT_ROUND > 0 and typ.upper() in ("FLOAT", "DOUBLE", "REAL"):
        return f'round("{col}", {FLOAT_ROUND})'
    return f'"{col}"'


def main():
    views = [a for a in sys.argv[1:] if not a.startswith("-")] or \
        os.environ.get("VIEWS", "").split() or DEFAULT_VIEWS
    con = duckdb.connect()

    def reg(engine, view):
        files = live_files(f"{BASE}/{engine}/gold/{view}")
        if not files:
            return None
        name = f"{engine}_{view}"
        con.register(name, pq.read_table(files))
        return name

    for v in views:
        print("=" * 72)
        print(f"VIEW: {v}")
        try:
            d, f = reg("dbspnet", v), reg("feldera", v)
        except Exception as e:  # noqa: BLE001
            print(f"  READ ERROR: {e}")
            continue
        if d is None or f is None:
            print(f"  MISSING: dbspnet={d} feldera={f}")
            continue

        dn = con.execute(f"SELECT count(*) FROM {d}").fetchone()[0]
        fn = con.execute(f"SELECT count(*) FROM {f}").fetchone()[0]
        rowflag = "" if dn == fn else "   <-- ROW COUNT DIFFERS"
        print(f"  rows: dbspnet={dn}  feldera={fn}{rowflag}")

        desc = con.execute(f"DESCRIBE SELECT * FROM {d}").fetchall()
        cols = [(r[0], r[1]) for r in desc]
        print(f"  {'column':<28}{'type':<14}{'diff':>10}")
        print("  " + "-" * 52)
        for c, t in cols:
            e = expr(c, t)
            try:
                dd = con.execute(
                    f"SELECT count(*) FROM (SELECT {e} AS x FROM {d} EXCEPT ALL SELECT {e} AS x FROM {f})"
                ).fetchone()[0]
                fd = con.execute(
                    f"SELECT count(*) FROM (SELECT {e} AS x FROM {f} EXCEPT ALL SELECT {e} AS x FROM {d})"
                ).fetchone()[0]
            except Exception as ex:  # noqa: BLE001
                print(f"  {c:<28}{t:<14}{'ERR':>10}  {ex}")
                continue
            diff = dd + fd
            mark = "" if diff == 0 else ("  sk (expected)" if c.lower().startswith("sk_") else "  <-- DIFF")
            print(f"  {c:<28}{t[:13]:<14}{diff:>10}{mark}")
            if diff and not c.lower().startswith("sk_"):
                # CAST samples to VARCHAR so DuckDB need not marshal TIMESTAMP/DATE
                # values into Python (which requires pytz); also keeps output readable.
                ds = con.execute(f"SELECT CAST({e} AS VARCHAR) FROM {d} EXCEPT ALL SELECT CAST({e} AS VARCHAR) FROM {f} LIMIT 4").fetchall()
                fs = con.execute(f"SELECT CAST({e} AS VARCHAR) FROM {f} EXCEPT ALL SELECT CAST({e} AS VARCHAR) FROM {d} LIMIT 4").fetchall()
                print(f"      dbspnet-only e.g.: {[r[0] for r in ds]}")
                print(f"      feldera-only e.g.: {[r[0] for r in fs]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
