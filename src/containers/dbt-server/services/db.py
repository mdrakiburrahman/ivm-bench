"""Database service — SQLite connection management."""

import os
import sqlite3
import threading

STATE_DIR = os.environ.get("STATE_DIR", "/data/state")
DB_PATH = os.path.join(STATE_DIR, "state.db")
DB_LOCK = threading.Lock()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(STATE_DIR, exist_ok=True)
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
