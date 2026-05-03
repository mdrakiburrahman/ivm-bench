"""Database service — in-memory SQLite for ephemeral run state.

Uses shared-cache in-memory SQLite so multiple connections see the same
database.  A keep-alive connection prevents the in-memory DB from being
garbage-collected.  State is intentionally ephemeral — it dies with the
container.  Persistent results are pulled by benchmark-server.
"""

import os
import sqlite3
import threading

STATE_DIR = os.environ.get("STATE_DIR", "/data/state")

# Shared-cache in-memory URI — all connections share one in-memory database
_DB_URI = "file:dbt_memdb?mode=memory&cache=shared"

DB_LOCK = threading.Lock()

# Keep-alive connection: prevents the in-memory database from being destroyed
# when the last regular connection closes.
_KEEP_ALIVE: sqlite3.Connection | None = None


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_URI, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    global _KEEP_ALIVE
    os.makedirs(STATE_DIR, exist_ok=True)

    # Open the keep-alive connection first
    _KEEP_ALIVE = sqlite3.connect(_DB_URI, uri=True)

    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            engine TEXT NOT NULL,
            scale_factor INTEGER NOT NULL,
            full_refresh INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            duration_s REAL,
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS run_nodes (
            run_id TEXT NOT NULL,
            unique_id TEXT NOT NULL,
            name TEXT NOT NULL,
            resource_type TEXT,
            execution_time_s REAL,
            status TEXT,
            compiled_sql TEXT,
            depends_on TEXT,
            rows_affected INTEGER,
            PRIMARY KEY (run_id, unique_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    conn.close()
