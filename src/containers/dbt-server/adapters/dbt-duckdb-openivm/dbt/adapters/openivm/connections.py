"""Connection manager — ALL SQL executes via the OpenIVM CLI binary."""

import csv
import json
import logging
import os
import re
import signal
import subprocess
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Tuple

import agate
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError

logger = logging.getLogger(__name__)

OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")
WORK_DIR = os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm")

# Per-CLI-session memory cap and spill directory. Defaults are sized for
# the serial-mode single-engine container (124 GB host, ~100 GB peak
# observed at SF=100). When running in parallel mode, the orchestrator
# overrides DUCKDB_OPENIVM_MEM_LIMIT to a fraction of the per-engine
# container so the four engines don't collectively oversubscribe RAM.
MEM_LIMIT = os.environ.get("DUCKDB_OPENIVM_MEM_LIMIT", "115GB")
TEMP_DIR = os.environ.get(
    "DUCKDB_OPENIVM_TEMP_DIR",
    os.path.join(WORK_DIR, "_tmp"),
)
THREADS = os.environ.get("DUCKDB_OPENIVM_THREADS", "")
PROFILE_REFRESH = os.environ.get("OPENIVM_PROFILE_REFRESH", "0") == "1"


TRACE_WAITS = os.environ.get("OPENIVM_TRACE_WAITS", "0") == "1"
# OAT cleanup wipes /data/logs before artifacts are uploaded; per-engine stats survive.
WAIT_TRACE_DIR = Path(os.environ.get("OPENIVM_WAIT_TRACE_DIR", "/data/stats/wait-traces"))
WAIT_SAMPLE_INTERVAL_S = 0.5
CGROUP_MEMORY_KEYS = ("anon", "file", "file_dirty", "file_writeback", "pgscan", "pgsteal",
                      "workingset_refault_file")


def _optional_proc_file(path):
    try:
        return Path(path).read_text()
    except OSError as error:
        return f"unavailable: {error}"


def _process_group_threads(pgid, proc_root=Path("/proc")):
    """Count threads of one process group by kernel state and wait channel."""
    counts = {}
    for stat_path in proc_root.glob("[0-9]*/task/[0-9]*/stat"):
        try:
            fields = stat_path.read_text().rsplit(")", 1)[1].split()
            if int(fields[2]) != pgid:
                continue
            wchan = (stat_path.parent / "wchan").read_text().strip() or "0"
        except (OSError, IndexError, ValueError):
            continue  # Threads exit between listing and reading.
        key = f"{fields[0]}:{wchan}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _host_sample(pgid=None):
    # Memory, PSI and disk counters are host-wide; cgroup files are per container.
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    cgroup_memory = {}
    for line in _optional_proc_file("/sys/fs/cgroup/memory.stat").splitlines():
        key, _, value = line.partition(" ")
        if key in CGROUP_MEMORY_KEYS:
            cgroup_memory[key] = int(value)
    disks = {}
    for line in _optional_proc_file("/proc/diskstats").splitlines():
        fields = line.split()
        if len(fields) >= 14 and fields[7] != "0":
            # writes completed, sectors written, write ms, in flight, io ticks ms, weighted ms
            disks[fields[2]] = [int(fields[i]) for i in (7, 9, 10, 11, 12, 13)]
    sample = {"timestamp_s": time.time(),
              "dirty_kb": int(memory["Dirty"].split()[0]),
              "writeback_kb": int(memory["Writeback"].split()[0]),
              "io_pressure": _optional_proc_file("/proc/pressure/io"),
              "cgroup_memory": cgroup_memory,
              "cgroup_io_stat": _optional_proc_file("/sys/fs/cgroup/io.stat"),
              "diskstats": disks}
    if pgid is not None:
        sample["threads"] = _process_group_threads(pgid)
    return sample


def _run_cli_wait_trace(command, program, trace_path, sql, attempt):
    """Opt-in Linux diagnostics; each invocation owns its files and process group."""
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    stop = threading.Event()

    with trace_path.with_suffix(".host.jsonl").open("x") as samples:
        vm = {name: _optional_proc_file(f"/proc/sys/vm/{name}").strip() for name in (
            "dirty_background_bytes", "dirty_background_ratio", "dirty_bytes", "dirty_ratio",
            "dirty_expire_centisecs", "dirty_writeback_centisecs")}
        samples.write(json.dumps({
            "sql": sql[:200], "attempt": attempt, **_host_sample(), "vm": vm,
            "memory_max": _optional_proc_file("/sys/fs/cgroup/memory.max").strip(),
            "cpu_max": _optional_proc_file("/sys/fs/cgroup/cpu.max"),
            "cpuset_cpus_effective": _optional_proc_file("/sys/fs/cgroup/cpuset.cpus.effective"),
            "mountinfo": _optional_proc_file("/proc/self/mountinfo"),
        }) + "\n")
        samples.flush()

        def sample_loop(pgid):
            while not stop.wait(WAIT_SAMPLE_INTERVAL_S):
                try:
                    samples.write(json.dumps(_host_sample(pgid)) + "\n")
                    samples.flush()
                except OSError as error:
                    logger.warning("OpenIVM host sampling failed: %s", error)
                    return

        # seccomp-bpf avoids trapping unrelated syscalls. Only durability barriers
        # and whole-file locks are traced: high-frequency fcntl/sleep tracing added
        # large overhead. Lock and throttling waits appear in the thread samples.
        traced = ["strace", "-f", "--seccomp-bpf", "-ttt", "-T", "-y",
                  "-e", "trace=fsync,fdatasync,sync_file_range,syncfs,msync,flock",
                  "-o", str(trace_path.with_suffix(".strace")), "--", *command]
        with subprocess.Popen(traced, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True, start_new_session=True) as proc:
            # start_new_session makes strace the group leader for itself and the CLI.
            sampler = threading.Thread(target=sample_loop, args=(proc.pid,), daemon=True)
            sampler.start()
            try:
                output, _ = proc.communicate(program, timeout=3600)
                return subprocess.CompletedProcess(command, proc.returncode, output)
            finally:
                # Killing only strace on timeout could leave a mutating CLI alive.
                if proc.poll() is None:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # The traced process exited between poll and kill.
                    proc.communicate()
                stop.set()
                sampler.join()


MAX_RETRIES = int(os.environ.get("OPENIVM_MAX_RETRIES", "10"))
RETRY_BACKOFF = float(os.environ.get("OPENIVM_RETRY_BACKOFF", "3.0"))


def _run_cli(sql: str, expect_output: bool = False) -> str:
    """Retry setup locks only: SQL may commit writes before returning an error."""
    db_file = os.path.join(WORK_DIR, "openivm.duckdb")
    meta_path = os.path.join(WORK_DIR, "openivm.ducklake")
    data_path = os.path.join(WORK_DIR, "data")
    os.makedirs(TEMP_DIR, exist_ok=True)

    preamble_lines = [
        ".bail on",
        ".timer on" if PROFILE_REFRESH else ".timer off",
        f"SET memory_limit='{MEM_LIMIT}';",
        f"SET temp_directory='{TEMP_DIR}';",
    ]
    if THREADS:
        preamble_lines.append(f"SET threads={int(THREADS)};")
    preamble_lines.extend([
        "LOAD openivm;",
    ])
    if PROFILE_REFRESH:
        preamble_lines.append("SET openivm_profile_refresh=true;")
    preamble_lines.extend([
        "SET openivm_cascade_refresh='off';",
        "INSTALL icu; LOAD icu;",
        "INSTALL ducklake; LOAD ducklake;",
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH '{data_path}', data_inlining_row_limit 0);",
    ])
    preamble = "\n".join(preamble_lines) + "\n"

    # CLI dot commands run outside the SQL statement. With .bail enabled,
    # these delimit setup, SQL execution, and teardown without parsing SQL or
    # assuming a failed multi-statement/native operation rolled back its writes.
    token = uuid.uuid4().hex
    started_marker = f"OPENIVM_SQL_STARTED_{token}"
    finished_marker = f"OPENIVM_SQL_FINISHED_{token}"
    program = f"{preamble}.print {started_marker}\n{sql}\n;\n.print {finished_marker}\n"
    for attempt in range(MAX_RETRIES + 1):
        cli_started = time.monotonic()
        if TRACE_WAITS:
            proc = _run_cli_wait_trace(
                [OPENIVM_BIN, db_file], program,
                WAIT_TRACE_DIR / f"{token}-{attempt}",
                sql, attempt,
            )
        else:
            proc = subprocess.run(
                [OPENIVM_BIN, db_file],
                input=program,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=3600,
            )
        cli_wall = time.monotonic() - cli_started
        lines = (proc.stdout or "").splitlines(keepends=True)
        if PROFILE_REFRESH:
            setup_seconds = sql_seconds = 0.0
            in_sql = False
            clean_lines = []
            for line in lines:
                if line.strip() == started_marker:
                    in_sql = True
                timing = re.fullmatch(r"Run Time \(s\): real ([0-9.]+) user [0-9.]+ sys [0-9.]+\s*", line)
                if timing:
                    if in_sql:
                        sql_seconds += float(timing[1])
                    else:
                        setup_seconds += float(timing[1])
                else:
                    clean_lines.append(line)
            lines = clean_lines
            # Export alongside native profiles; no extra database writes or commits.
            try:
                with open(os.path.join(TEMP_DIR, "cli-timings.csv"), "a", newline="") as trace:
                    csv.writer(trace).writerow([
                        time.time(), sql[:200], attempt, proc.returncode, cli_wall,
                        setup_seconds, sql_seconds, cli_wall - setup_seconds - sql_seconds,
                    ])
            except OSError as error:
                logger.warning("Could not record OpenIVM CLI timings: %s", error)
        sql_started = any(line.strip() == started_marker for line in lines)
        sql_finished = any(line.strip() == finished_marker for line in lines)
        output = "".join(line for line in lines if line.strip() not in (started_marker, finished_marker))
        if proc.returncode == 0 and sql_started and sql_finished:
            return output

        if proc.returncode > 0 and "database is locked" in output and not sql_started and attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF * (2 ** attempt)
            logger.warning(
                "OpenIVM CLI setup locked (attempt %d/%d), retrying in %.1fs: %s",
                attempt + 1, MAX_RETRIES + 1, wait, output[-2000:],
            )
            time.sleep(wait)
            continue

        phase = "after SQL completion" if sql_finished else "during SQL execution" if sql_started else "during setup"
        retry_detail = (
            " SQL execution started; not replaying because writes may already have committed."
            if sql_started else ""
        )
        raise DbtDatabaseError(
            f"OpenIVM CLI failed {phase} (rc={proc.returncode}, attempt {attempt + 1}/{MAX_RETRIES + 1})."
            f"{retry_detail}\n{output[-2000:]}"
        )


class OpenIVMConnectionManager(SQLConnectionManager):
    """Executes all SQL through the OpenIVM CLI binary subprocess."""

    TYPE = "openivm"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except DbtDatabaseError:
            raise
        except Exception as e:
            raise DbtDatabaseError(str(e))

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection
        # No persistent handle — each execute() is a subprocess call
        connection.handle = "openivm_cli"
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        connection.state = ConnectionState.CLOSED
        return connection

    def cancel(self, connection: Connection):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        return AdapterResponse(_message="OK", code="SUCCESS")

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql_stripped = sql.strip()
        if not sql_stripped or sql_stripped == ";":
            return AdapterResponse(_message="OK", code="SUCCESS"), agate.Table(rows=[])

        logger.debug("OpenIVM execute: %s", sql_stripped[:120])

        t0 = time.monotonic()
        output = _run_cli(sql_stripped)
        elapsed = time.monotonic() - t0

        logger.debug("OpenIVM result (%.2fs): %s", elapsed, output[:200])

        response = AdapterResponse(
            _message=f"OK ({elapsed:.2f}s)",
            rows_affected=0,
            code="SUCCESS",
        )

        # Parse tabular output if fetch requested
        if fetch and output.strip():
            table = self._parse_cli_output(output)
        else:
            table = agate.Table(rows=[])

        return response, table

    def _parse_cli_output(self, output: str) -> agate.Table:
        """Parse pipe-delimited CLI output into an agate Table."""
        lines = [l.strip() for l in output.strip().split("\n") if l.strip()]
        if not lines:
            return agate.Table(rows=[])

        # DuckDB CLI outputs pipe-delimited tables with a header separator
        # Format: col1|col2\n---|---\nval1|val2
        if len(lines) >= 2 and all(c in "-|+ " for c in lines[1]):
            headers = [h.strip() for h in lines[0].split("|")]
            rows = []
            for line in lines[2:]:
                vals = [v.strip() for v in line.split("|")]
                rows.append(dict(zip(headers, vals)))
            return agate.Table.from_object(rows) if rows else agate.Table(rows=[], column_names=headers)

        return agate.Table(rows=[])
