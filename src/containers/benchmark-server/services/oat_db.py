"""OAT-specific SQLite schema add-ons.

Two new tables, both keyed by the OAT run id (separate from
``benchmark_runs.id``):

  oat_runs        — one row per OAT sweep (1..N experiments)
  oat_experiments — one row per experiment inside the sweep; FK to both
                    oat_runs.id and benchmark_runs.id

Each individual experiment still writes a ``benchmark_runs`` row + per-engine
``engine_batches`` rows via the existing schema. ``oat_experiments`` just
adds parent + linkage metadata on top, so:

  * single-experiment runs continue to work unchanged (no oat_runs row written)
  * OAT runs get the parent in ``oat_runs`` plus N child rows in
    ``oat_experiments``, each pointing at its own ``benchmark_runs`` row

``init_db()`` here is called from ``services/db.init_db()`` at server start,
so the tables exist with no separate migration step.
"""

import os
import sqlite3
import threading

STATE_DIR = os.environ.get("BENCHMARK_STATE_DIR", "/data/state")
DB_PATH = os.path.join(STATE_DIR, "benchmark.db")
DB_LOCK = threading.Lock()


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Create ``oat_runs`` + ``oat_experiments`` tables if missing.

    Called from services.db.init_db() during server startup. Safe to call
    repeatedly; both tables use CREATE TABLE IF NOT EXISTS.
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = _get_db()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS oat_runs (
                id                TEXT PRIMARY KEY,
                status            TEXT NOT NULL DEFAULT 'pending',
                started_at        TEXT,
                completed_at      TEXT,
                total_duration_s  REAL,
                experiments_file  TEXT,
                error             TEXT
            );

            CREATE TABLE IF NOT EXISTS oat_experiments (
                oat_run_id        TEXT NOT NULL,
                exp_idx           INTEGER NOT NULL,
                benchmark_id      TEXT,
                label             TEXT,
                status            TEXT NOT NULL DEFAULT 'pending',
                started_at        TEXT,
                ended_at          TEXT,
                wall_clock_s      REAL,
                disk_free_pct     REAL,
                inputs_json       TEXT,
                outputs_json      TEXT,
                error             TEXT,
                PRIMARY KEY (oat_run_id, exp_idx),
                FOREIGN KEY (oat_run_id)   REFERENCES oat_runs(id),
                FOREIGN KEY (benchmark_id) REFERENCES benchmark_runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_oat_experiments_run
                ON oat_experiments(oat_run_id, exp_idx);
        """)
        conn.commit()
    finally:
        conn.close()
