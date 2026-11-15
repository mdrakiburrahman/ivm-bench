#!/usr/bin/env python3
"""Correctness check: diff DbspNet's gold output tables against Feldera's.

Both engines write Delta tables to mount/results/<SF>/<engine>/gold/<view>. Each is read
as its CURRENT logical contents by gold_tables.py: DbspNet's deletion vectors applied,
Feldera's __feldera_op change log netted, timestamps compared as UTC instants. Read raw,
both engines' retractions look like extra rows. That raw read is what produced July 2026's
false "Feldera duplicates" and "SCD tie" verdicts (dbsp-net issue #6).

For each of the 16 gold views it reports:
  * row counts on each side
  * exact_diff       — mismatched rows over ALL columns
  * data_diff        — mismatched rows EXCLUDING sk_* surrogate-key columns

sk_* are md5(natural_key + '-' + ...) and only match cross-engine if both format
the md5 inputs identically (esp. timestamp -> string). So exact_diff>0 with
data_diff==0 means "same data, surrogate-key formatting differs" — not a
correctness problem. data_diff>0 is a real divergence.

Run (from repo root, after a run with PRESERVE_RESULTS=1):

    docker run --rm -w /work/src/.scripts \
      -v "$PWD/mount:/mount:ro" -v "$PWD/src/.scripts:/work/src/.scripts:ro" \
      python:3.11-slim bash -c "pip install --quiet 'duckdb<2' && SF=3 python3 compare_outputs.py"

or natively, in any venv with ``duckdb<2``:
``RESULTS_BASE=$PWD/mount/results/3 python src/.scripts/compare_outputs.py``.
DuckDB 2.0 changed its connection API and breaks these scripts, hence the pin.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gold_tables  # noqa: E402

SF = os.environ.get("SF", "3")
BASE = os.environ.get("RESULTS_BASE", f"/mount/results/{SF}")
FLOAT_ROUND = int(os.environ.get("FLOAT_ROUND", "6"))

VIEWS = [
    "dim_account", "dim_broker", "dim_company", "dim_customer", "dim_date",
    "dim_security", "dim_trade", "fact_cash_balances", "fact_cash_transactions",
    "fact_holdings", "fact_trade", "fact_watches", "trade_volume_stats",
    "market_volatility", "customer_concentration", "broker_performance",
]



def main() -> int:
    con = gold_tables.connect()

    def rel(engine, view):
        return gold_tables.register(con, BASE, engine, view)

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
        try:
            d, f = rel("dbspnet", v), rel("feldera", v)
            if d is None or f is None:
                miss = " ".join(x for x, r in (("dbspnet", d), ("feldera", f)) if r is None)
                print(f"{v:<24} MISSING ({miss})")
                all_ok = False
                continue
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
