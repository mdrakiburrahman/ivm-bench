#!/usr/bin/env python3
"""Correctness check: diff DbspNet's gold output tables against Feldera's.

Both engines write full-state (mode: truncate) Delta tables to
mount/results/<SF>/<engine>/gold/<view>. We resolve each table's CURRENT live
parquet files straight from its _delta_log (replaying add/remove across commits,
so superseded truncate files are dropped) and read those with DuckDB's plain
read_parquet — NO delta extension, so engineered-wood's / Feldera's _delta_log
quirks (which delta-kernel-rs rejects) don't matter.

For each of the 16 gold views it reports:
  * row counts on each side
  * exact_diff       — mismatched rows over ALL columns
  * data_diff        — mismatched rows EXCLUDING sk_* surrogate-key columns

sk_* are md5(natural_key + '-' + ...) and only match cross-engine if both format
the md5 inputs identically (esp. timestamp -> string). So exact_diff>0 with
data_diff==0 means "same data, surrogate-key formatting differs" — not a
correctness problem. data_diff>0 is a real divergence.

Run (from repo root, after a run with PRESERVE_RESULTS=1):

    docker run --rm -w /work \
      -v "$PWD/mount:/mount:ro" \
      -v "$PWD/src/.scripts/compare_outputs.py:/work/compare_outputs.py:ro" \
      python:3.11-slim bash -c "pip install --quiet duckdb && SF=3 python3 compare_outputs.py"
"""
import glob
import json
import os
import sys
import urllib.parse

import duckdb

SF = os.environ.get("SF", "3")
BASE = os.environ.get("RESULTS_BASE", f"/mount/results/{SF}")
FLOAT_ROUND = int(os.environ.get("FLOAT_ROUND", "6"))

VIEWS = [
    "dim_account", "dim_broker", "dim_company", "dim_customer", "dim_date",
    "dim_security", "dim_trade", "fact_cash_balances", "fact_cash_transactions",
    "fact_holdings", "fact_trade", "fact_watches", "trade_volume_stats",
    "market_volatility", "customer_concentration", "broker_performance",
]


def live_files(table_dir):
    """Current live parquet files, from replaying the _delta_log add/remove log."""
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


def main() -> int:
    con = duckdb.connect()

    def rel(engine, view):
        files = live_files(f"{BASE}/{engine}/gold/{view}")
        if not files:
            return None
        lst = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
        return f"read_parquet([{lst}])"

    def project(r, cols, types):
        sel = []
        for c, t in zip(cols, types):
            if FLOAT_ROUND > 0 and t.upper() in ("FLOAT", "DOUBLE", "REAL"):
                sel.append(f'round("{c}", {FLOAT_ROUND}) AS "{c}"')
            else:
                sel.append(f'"{c}"')
        return f"SELECT {', '.join(sel)} FROM {r}"

    hdr = f"{'view':<24}{'dbspnet':>10}{'feldera':>10}{'exact_diff':>12}{'data_diff':>11}"
    print(hdr)
    print("-" * len(hdr))
    all_ok = True

    for v in VIEWS:
        d, f = rel("dbspnet", v), rel("feldera", v)
        if d is None or f is None:
            print(f"{v:<24} MISSING ({'dbspnet' if d is None else ''}{' feldera' if f is None else ''})")
            all_ok = False
            continue
        try:
            dn = con.execute(f"SELECT count(*) FROM {d}").fetchone()[0]
            fn = con.execute(f"SELECT count(*) FROM {f}").fetchone()[0]
            desc = con.execute(f"DESCRIBE SELECT * FROM {d}").fetchall()
        except Exception as e:  # noqa: BLE001
            print(f"{v:<24} READ ERROR: {e}")
            all_ok = False
            continue

        cols = [r[0] for r in desc]
        types = [r[1] for r in desc]
        dp, fp = project(d, cols, types), project(f, cols, types)
        exact = (con.execute(f"SELECT count(*) FROM (({dp}) EXCEPT ALL ({fp}))").fetchone()[0]
                 + con.execute(f"SELECT count(*) FROM (({fp}) EXCEPT ALL ({dp}))").fetchone()[0])

        nk = [(c, t) for c, t in zip(cols, types) if not c.lower().startswith("sk_")]
        ncols, ntypes = [c for c, _ in nk], [t for _, t in nk]
        dpn, fpn = project(d, ncols, ntypes), project(f, ncols, ntypes)
        data = (con.execute(f"SELECT count(*) FROM (({dpn}) EXCEPT ALL ({fpn}))").fetchone()[0]
                + con.execute(f"SELECT count(*) FROM (({fpn}) EXCEPT ALL ({dpn}))").fetchone()[0])

        flag = "" if data == 0 else "  <-- DATA DIFF"
        print(f"{v:<24}{dn:>10}{fn:>10}{exact:>12}{data:>11}{flag}")
        if data != 0 or dn != fn:
            all_ok = False

    print()
    print("PASS: every view matches Feldera on data (surrogate keys aside)."
          if all_ok else "DIFFERENCES found — inspect the views flagged above.")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
