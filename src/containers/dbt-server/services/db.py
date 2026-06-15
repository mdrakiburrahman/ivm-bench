"""Database service — file-based SQLite for ephemeral run state.

Uses a file-backed database in WAL mode so multiple connections (the
HTTP request threads, the SSE stream loop, and the dbt-runner background
thread) can read and write concurrently without tripping
``SQLITE_LOCKED`` from shared-cache table-level locking.

State is intentionally ephemeral — the file lives on the container's
local filesystem (not a mounted volume) and is wiped on container start.
Persistent results are pulled by benchmark-server.
"""

import os
import sqlite3
import threading

STATE_DIR = os.environ.get("STATE_DIR", "/data/state")

# File-backed SQLite. Kept on container-local FS (/tmp), NOT under
# /data/state, so each dbt-server container has its own independent DB
# even when several share a host (parallel mode). Wiped on container
# start by ``init_db``.
_DB_PATH = os.environ.get("DBT_SERVER_DB_PATH", "/tmp/dbt_server_state.sqlite")
_DB_URI = f"file:{_DB_PATH}"

# Module-level lock kept for backwards compatibility with code paths that
# already serialize their writes through ``DB_LOCK``. WAL handles real
# concurrency on its own; this lock is a belt-and-braces.
DB_LOCK = threading.Lock()

# Keep-alive connection: opens the database once at startup so PRAGMAs
# (journal_mode=WAL, synchronous=NORMAL) survive even if every other
# connection closes. Also useful for bookkeeping.
_KEEP_ALIVE: sqlite3.Connection | None = None

# Busy timeout in milliseconds. Applied to every connection so writers
# wait instead of erroring when readers briefly hold locks.
_BUSY_TIMEOUT_MS = 30000


def _configure(conn: sqlite3.Connection) -> None:
    """Apply per-connection PRAGMAs (WAL-friendly defaults)."""
    conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous = NORMAL")


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_URI, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    _configure(conn)
    return conn


def init_db():
    global _KEEP_ALIVE
    os.makedirs(STATE_DIR, exist_ok=True)

    # Wipe any leftover DB from a prior container instance so each run
    # starts from clean state. Also remove WAL/SHM sidecar files.
    for suffix in ("", "-wal", "-shm", "-journal"):
        try:
            os.remove(_DB_PATH + suffix)
        except FileNotFoundError:
            pass

    # Open the keep-alive connection first and switch to WAL.
    _KEEP_ALIVE = sqlite3.connect(_DB_URI, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000)
    _configure(_KEEP_ALIVE)
    _KEEP_ALIVE.execute("PRAGMA journal_mode = WAL")
    _KEEP_ALIVE.commit()

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
            message TEXT,
            PRIMARY KEY (run_id, unique_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    conn.close()
