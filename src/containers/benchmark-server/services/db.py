"""Persistent SQLite state for benchmark-server.

Stores benchmark run metadata and per-engine/batch results so that state
survives dbt-server container restarts.
"""

import os
import sqlite3
import threading

STATE_DIR = os.environ.get("BENCHMARK_STATE_DIR", "/data/state")
DB_PATH = os.path.join(STATE_DIR, "benchmark.db")
DB_LOCK = threading.Lock()


def get_db() -> sqlite3.Connection:
    # STATE_DIR is a bind-mount source that can disappear mid-sweep; recreate it
    # so connect() never raises "unable to open database file" and abort a run.
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS benchmark_runs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            started_at TEXT,
            completed_at TEXT,
            total_duration_s REAL,
            config_json TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS engine_batches (
            benchmark_id TEXT NOT NULL,
            engine TEXT NOT NULL,
            batch_num INTEGER NOT NULL,
            run_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            duration_s REAL,
            result_json TEXT,
            error TEXT,
            PRIMARY KEY (benchmark_id, engine, batch_num),
            FOREIGN KEY (benchmark_id) REFERENCES benchmark_runs(id)
        );
    """)
    conn.commit()
    conn.close()

    # OAT harness tables: oat_runs + oat_experiments. Kept in a separate
    # module so the OAT schema stays cleanly removable / iterable.
    from services import oat_db
    oat_db.init_db()
