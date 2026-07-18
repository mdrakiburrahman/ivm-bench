#!/usr/bin/env python3
"""Correctness check: diff DbspNet's gold output tables against Feldera's.

Both engines write full-state (mode: truncate) Delta tables to
mount/results/<SF>/<engine>/gold/<view>. This reads the CURRENT snapshot of each
(via DuckDB's delta extension, so superseded truncate files are ignored) and
compares them set-wise (EXCEPT ALL both directions). For each of the 16 gold
views it reports:

  * row counts on each side
  * exact_diff       — mismatched rows over ALL columns
  * data_diff        — mismatched rows EXCLUDING sk_* surrogate-key columns

Surrogate keys are md5(natural_key + '-' + ...) and only match cross-engine if
both format the md5 inputs identically (esp. timestamp -> string), so a nonzero
exact_diff with a zero data_diff means "same data, surrogate-key formatting
differs" — not a correctness problem. A nonzero data_diff is a real divergence.

Run (from the repo root, after a run with PRESERVE_RESULTS=1):

    docker run --rm -w /work \
      -v "$PWD/mount:/mount:ro" \
      -v "$PWD/src/.scripts/compare_outputs.py:/work/compare_outputs.py:ro" \
      python:3.11-slim bash -c "pip install --quiet duckdb && SF=3 python3 compare_outputs.py"

Set SF to the scale factor you ran. FLOAT_TOLERANCE rounds float columns before
comparing (set to 0 for exact).
"""
import os
import sys

import duckdb

SF = os.environ.get("SF", "3")
BASE = os.environ.get("RESULTS_BASE", f"/mount/results/{SF}")
FLOAT_ROUND = int(os.environ.get("FLOAT_ROUND", "6"))  # decimal places for float compare

VIEWS = [
    "dim_account", "dim_broker", "dim_company", "dim_customer", "dim_date",
    "dim_security", "dim_trade", "fact_cash_balances", "fact_cash_transactions",
    "fact_holdings", "fact_trade", "fact_watches", "trade_volume_stats",
    "market_volatility", "customer_concentration", "broker_performance",
]


def main() -> int:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    def scan(engine: str, view: str) -> str:
        return f"delta_scan('{BASE}/{engine}/gold/{view}')"

    def projected(rel: str, cols, types) -> str:
        # Round float columns so tiny precision differences don't count as diffs.
        sel = []
        for c, t in zip(cols, types):
            q = f'"{c}"'
            if FLOAT_ROUND > 0 and t.upper() in ("FLOAT", "DOUBLE", "REAL"):
                q = f'round("{c}", {FLOAT_ROUND}) AS "{c}"'
            sel.append(q)
        return f"SELECT {', '.join(sel)} FROM {rel}"

    hdr = f"{'view':<24}{'dbspnet':>10}{'feldera':>10}{'exact_diff':>12}{'data_diff':>11}"
    print(hdr)
    print("-" * len(hdr))
    all_ok = True

    for v in VIEWS:
        d, f = scan("dbspnet", v), scan("feldera", v)
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

        dp, fp = projected(d, cols, types), projected(f, cols, types)
        exact = (con.execute(f"SELECT count(*) FROM (({dp}) EXCEPT ALL ({fp}))").fetchone()[0]
                 + con.execute(f"SELECT count(*) FROM (({fp}) EXCEPT ALL ({dp}))").fetchone()[0])

        nk = [(c, t) for c, t in zip(cols, types) if not c.lower().startswith("sk_")]
        ncols, ntypes = [c for c, _ in nk], [t for _, t in nk]
        dpn, fpn = projected(d, ncols, ntypes), projected(f, ncols, ntypes)
        data = (con.execute(f"SELECT count(*) FROM (({dpn}) EXCEPT ALL ({fpn}))").fetchone()[0]
                + con.execute(f"SELECT count(*) FROM (({fpn}) EXCEPT ALL ({dpn}))").fetchone()[0])

        flag = "" if data == 0 else "  <-- DATA DIFF"
        print(f"{v:<24}{dn:>10}{fn:>10}{exact:>12}{data:>11}{flag}")
        if data != 0 or dn != fn:
            all_ok = False

    print()
    if all_ok:
        print("PASS: every view matches Feldera on data (surrogate keys aside).")
        return 0
    print("DIFFERENCES found — inspect the views flagged above.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
