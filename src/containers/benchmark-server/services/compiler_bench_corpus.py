"""compiler-bench corpus preparation.

The compiler benchmark asks one question per query: *given this SELECT, can the
engine maintain it incrementally, does the refresh run, and is the result
right?* That mirrors openivm's own `benchmark/src/rewriter_benchmark.cpp`, but
across every engine in this repo instead of DuckDB-OpenIVM alone.

The corpus is native DuckDB/OpenIVM SQL over a TPC-C schema.  DuckDB-family
engines receive that source SQL directly; routing their own benchmark through a
SQL translator would change what is being measured and can reject valid native
constructs.  Only engines with a different SQL dialect receive an LPTS-rendered
equivalent, planned through DuckDB's optimizer first.

Two properties make the artifact cacheable and engine-agnostic:

  * ``lpts_output_unqualified`` makes LPTS emit bare table names, so a single
    per-dialect corpus works for every engine sharing that dialect regardless of
    its catalog/schema layout — each engine sets its own session default before
    running. (Without it the *local* planning catalog would leak into the SQL.)
  * target rendering depends only on (corpus revision, lpts revision, dialect,
    schema), none of which vary per experiment.  The native DuckDB corpus does
    not depend on LPTS.

For cross-engine targets, queries LPTS cannot express in that target dialect are
recorded as ``translation_failed`` and reported as their own bucket. They are
NOT silently dropped: a query missing from an engine's corpus would otherwise
inflate that engine's success rate.  Native DuckDB/OpenIVM queries never enter
this bucket.

Outputs, under ``mount/compiler-bench/corpus/``:

    meta.json                     pins + counts + per-dialect totals
    common.txt                    queries available in EVERY requested dialect
    <dialect>/queries/<name>.sql  native or target-rendered query, one per file
    <dialect>/translation.csv     per-query status + error for the whole corpus
    <dialect>/schema.sql          base-table DDL in that dialect
    <dialect>/deltas.sql          delta-pool DML in that dialect
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from sql_catalog import load_sql

logger = logging.getLogger(__name__)

CORPUS_SUBDIR = "tpcc"


def _sql(engine: str, name: str, **params) -> str:
    return load_sql(
        engine, "inline", f"compiler_bench/benchmark_server_corpus/{name}", **params
    )

# Engine -> LPTS output dialect. Spark SQL covers the Spark-family engines
# (Databricks SQL and Fabric Spark are Spark SQL supersets for the constructs
# this corpus uses). Feldera has its own Calcite-based LPTS target because it
# supports aggregates such as ARG_MIN/ARG_MAX that PostgreSQL does not.
ENGINE_DIALECTS: Dict[str, str] = {
    "duckdb": "duckdb",
    "duckdb-openivm": "duckdb",
    "spark": "spark",
    "spark-openivm": "spark",
    "databricks-enzyme": "spark",
    "fabric-jvm-35": "spark",
    "fabric-openivm-jvm-35": "spark",
    "feldera": "feldera",
    "risingwave": "postgres",
}


def dialects_for(engines: Sequence[str]) -> List[str]:
    """Distinct dialects needed to cover ``engines``, in stable order."""
    seen: List[str] = []
    for engine in engines:
        dialect = ENGINE_DIALECTS.get(engine)
        if dialect and dialect not in seen:
            seen.append(dialect)
    return seen


# ---------------------------------------------------------------------------
# Canonical TPC-C schema
# ---------------------------------------------------------------------------
# Source of truth for the corpus schema, transcribed from openivm's
# `benchmark/src/tpcc_helpers.cpp` (CreateTPCCSchema / InsertTPCCData). It lives
# here rather than being read out of the openivm image because the *engines*
# need it too — every engine must materialize these tables before it can create
# a view over them, and each needs the DDL in its own type vocabulary.
#
# Column types use a portable vocabulary rendered per dialect by _render_type.
# scale_factor == number of warehouses, exactly as in the C++ helper, so a run
# here is comparable to a run of the original benchmark at the same scale.

TPCC_TABLES: Dict[str, List[Tuple[str, str]]] = {
    "WAREHOUSE": [
        ("W_ID", "INT"), ("W_YTD", "DECIMAL(12,2)"), ("W_TAX", "DECIMAL(4,4)"),
        ("W_NAME", "VARCHAR(10)"), ("W_STREET_1", "VARCHAR(20)"),
        ("W_STREET_2", "VARCHAR(20)"), ("W_CITY", "VARCHAR(20)"),
        ("W_STATE", "CHAR(2)"), ("W_ZIP", "CHAR(9)"),
    ],
    "DISTRICT": [
        ("D_W_ID", "INT"), ("D_ID", "INT"), ("D_YTD", "DECIMAL(12,2)"),
        ("D_TAX", "DECIMAL(4,4)"), ("D_NEXT_O_ID", "INT"), ("D_NAME", "VARCHAR(10)"),
        ("D_STREET_1", "VARCHAR(20)"), ("D_STREET_2", "VARCHAR(20)"),
        ("D_CITY", "VARCHAR(20)"), ("D_STATE", "CHAR(2)"), ("D_ZIP", "CHAR(9)"),
    ],
    "CUSTOMER": [
        ("C_W_ID", "INT"), ("C_D_ID", "INT"), ("C_ID", "INT"),
        ("C_DISCOUNT", "DECIMAL(4,4)"), ("C_CREDIT", "CHAR(2)"),
        ("C_LAST", "VARCHAR(16)"), ("C_FIRST", "VARCHAR(16)"),
        ("C_CREDIT_LIM", "DECIMAL(12,2)"), ("C_BALANCE", "DECIMAL(12,2)"),
        ("C_YTD_PAYMENT", "FLOAT"), ("C_PAYMENT_CNT", "INT"),
        ("C_DELIVERY_CNT", "INT"), ("C_STREET_1", "VARCHAR(20)"),
        ("C_STREET_2", "VARCHAR(20)"), ("C_CITY", "VARCHAR(20)"),
        ("C_STATE", "CHAR(2)"), ("C_ZIP", "CHAR(9)"), ("C_PHONE", "CHAR(16)"),
        ("C_SINCE", "TIMESTAMP"), ("C_MIDDLE", "CHAR(2)"), ("C_DATA", "VARCHAR(500)"),
    ],
    "ITEM": [
        ("I_ID", "INT"), ("I_NAME", "VARCHAR(24)"), ("I_PRICE", "DECIMAL(5,2)"),
        ("I_DATA", "VARCHAR(50)"), ("I_IM_ID", "INT"),
    ],
    "STOCK": [
        ("S_W_ID", "INT"), ("S_I_ID", "INT"), ("S_QUANTITY", "INT"),
        ("S_YTD", "DECIMAL(8,2)"), ("S_ORDER_CNT", "INT"), ("S_REMOTE_CNT", "INT"),
        ("S_DATA", "VARCHAR(50)"),
    ] + [(f"S_DIST_{i:02d}", "CHAR(24)") for i in range(1, 11)],
    "OORDER": [
        ("O_W_ID", "INT"), ("O_D_ID", "INT"), ("O_ID", "INT"), ("O_C_ID", "INT"),
        ("O_CARRIER_ID", "INT"), ("O_OL_CNT", "INT"), ("O_ALL_LOCAL", "INT"),
        ("O_ENTRY_D", "TIMESTAMP"),
    ],
    "NEW_ORDER": [("NO_W_ID", "INT"), ("NO_D_ID", "INT"), ("NO_O_ID", "INT")],
    "ORDER_LINE": [
        ("OL_W_ID", "INT"), ("OL_D_ID", "INT"), ("OL_O_ID", "INT"),
        ("OL_NUMBER", "INT"), ("OL_I_ID", "INT"), ("OL_DELIVERY_D", "TIMESTAMP"),
        ("OL_AMOUNT", "DECIMAL(6,2)"), ("OL_SUPPLY_W_ID", "INT"),
        ("OL_QUANTITY", "DECIMAL(6,2)"), ("OL_DIST_INFO", "CHAR(24)"),
    ],
    "HISTORY": [
        ("H_C_ID", "INT"), ("H_C_D_ID", "INT"), ("H_C_W_ID", "INT"),
        ("H_D_ID", "INT"), ("H_W_ID", "INT"), ("H_DATE", "TIMESTAMP"),
        ("H_AMOUNT", "DECIMAL(6,2)"), ("H_DATA", "VARCHAR(24)"),
    ],
}

# Per-warehouse cardinality, matching InsertTPCCData exactly.
TPCC_DISTRICTS_PER_WH = 10
TPCC_CUSTOMERS_PER_DISTRICT = 100
TPCC_ORDERS_PER_DISTRICT = 100
TPCC_ORDER_LINES_PER_ORDER = 10
TPCC_NUM_ITEMS = 1000


def _render_type(portable_type: str, dialect: str) -> str:
    """Render a portable column type in ``dialect``."""
    if dialect in ("spark", "hive"):
        # Spark has no length-parameterised VARCHAR/CHAR semantics worth
        # preserving here (and CHAR pads on read), so both map to STRING.
        if portable_type.startswith(("VARCHAR", "CHAR")):
            return "STRING"
        return portable_type
    if dialect in ("postgres", "feldera"):
        if portable_type == "FLOAT":
            return "REAL"
        if portable_type == "INT":
            return "INTEGER"
        return portable_type
    return portable_type


def tpcc_schema_ddl(engine: str, *, table_suffix: str = "") -> List[str]:
    # sql-catalog: exempt This docstring names the generated DDL, not executable SQL.
    """CREATE TABLE statements for the TPC-C schema in ``engine``."""
    dialect = ENGINE_DIALECTS[engine]
    return [
        _sql(
            engine,
            "tpcc_create_table",
            table=f"{table}{table_suffix}",
            columns=", ".join(
                f"{name} {_render_type(t, dialect)}" for name, t in columns
            ),
        )
        for table, columns in TPCC_TABLES.items()
    ]


def tpcc_load_sql_duckdb(scale_factor: int) -> List[str]:
    """Set-based TPC-C data generation, transcribed from InsertTPCCData.

    DuckDB-only: this runs once in the prep step to produce Parquet, which the
    engines then ingest. Keeping generation in one dialect keeps every engine's
    base data byte-identical instead of re-deriving it per engine.
    """
    n = scale_factor + 1
    d = TPCC_DISTRICTS_PER_WH + 1
    c = TPCC_CUSTOMERS_PER_DISTRICT + 1
    o = TPCC_ORDERS_PER_DISTRICT + 1
    ol = TPCC_ORDER_LINES_PER_ORDER + 1
    i = TPCC_NUM_ITEMS + 1
    engine = "duckdb-openivm"
    return [
        _sql(engine, "tpcc_load_warehouse", n=n),
        _sql(engine, "tpcc_load_district", n=n, d=d),
        _sql(engine, "tpcc_load_customer", n=n, d=d, c=c),
        _sql(engine, "tpcc_load_item", i=i),
        _sql(engine, "tpcc_load_stock", n=n, i=i),
        _sql(engine, "tpcc_load_oorder", n=n, d=d, c=c, o=o),
        _sql(engine, "tpcc_load_order_line", n=n, d=d, o=o, ol=ol),
    ]


def tpcc_delta_pool(scale_factor: int, engine: str) -> List[str]:
    """Portable delta pool mirroring openivm's GenerateDeltaPool.

    Same 500 statements, same mt19937-equivalent mix ratios and seed-42
    determinism, with one deliberate difference: the original deletes a single
    NEW_ORDER row via DuckDB's `rowid` pseudo-column, which no other engine
    has. Here the DELETE is expressed against the (NO_W_ID, NO_D_ID, NO_O_ID)
    key instead. That can remove more than one row where the original removed
    exactly one, so delete-delta *volumes* are not comparable statement-for-
    statement with a C++ run — the incrementalizability verdict, which is what
    this benchmark reports, is unaffected.
    """
    rng = random.Random(42)
    deltas: List[str] = []
    for i in range(500):
        kind = rng.randint(0, 99)
        w = rng.randint(1, max(1, scale_factor))
        d = rng.randint(1, 10)
        c = rng.randint(1, 30)
        item = rng.randint(1, 100)
        amt = rng.randint(50, 500)
        balance = -1.0 * amt
        qty = 50 + (i % 50)
        if kind < 40:
            deltas.append(
                _sql(
                    engine, "tpcc_delta_update_customer", balance=balance,
                    payment_count=i % 10, w=w, d=d, c=c,
                )
            )
        elif kind < 60:
            deltas.append(
                _sql(
                    engine, "tpcc_delta_update_stock", quantity=qty,
                    order_count=i % 20, w=w, item=item,
                )
            )
        elif kind < 75:
            deltas.append(
                _sql(
                    engine, "tpcc_delta_update_order_line", w=w, d=d,
                    date="2026-01-01 00:00:00",
                )
            )
        elif kind < 85:
            deltas.append(
                _sql(
                    engine, "tpcc_delta_insert_history", c=c, d=d,
                    date="2026-01-01 00:00:00", amt=amt,
                )
            )
        elif kind < 90:
            deltas.append(_sql(engine, "tpcc_delta_insert_new_order", w=w, d=d))
        elif kind < 95:
            deltas.append(_sql(engine, "tpcc_delta_delete_new_order", w=w, d=d))
        else:
            deltas.append(_sql(engine, "tpcc_delta_update_warehouse", ytd=amt * 100, w=w))
    return deltas


# ---------------------------------------------------------------------------
# Corpus reading
# ---------------------------------------------------------------------------

_META_RE = re.compile(r"^\s*--\s*(\{.*\})\s*$")


@dataclass
class CorpusQuery:
    name: str
    sql: str
    meta: dict

    @property
    def meta_is_incremental(self) -> Optional[bool]:
        """The corpus author's prediction, or None when absent.

        Reported alongside each engine's verdict so the metadata-vs-engine
        confusion matrix from the C++ benchmark carries over.
        """
        value = self.meta.get("is_incremental")
        return bool(value) if isinstance(value, bool) else None


def read_corpus(queries_dir: Path) -> List[CorpusQuery]:
    """Read every query file in ``queries_dir``.

    Remove the ``dl.`` catalog qualifier from DuckLake variants: it names
    storage in OpenIVM's source benchmark, not part of the query semantics, and
    no other benchmark engine has that catalog. The query itself remains useful
    cross-engine coverage.
    """
    queries: List[CorpusQuery] = []
    for path in sorted(queries_dir.glob("*.sql")):
        text = path.read_text(encoding="utf-8", errors="replace")
        meta: dict = {}
        body_lines: List[str] = []
        for line in text.splitlines():
            match = _META_RE.match(line)
            if match and not meta:
                try:
                    meta = json.loads(match.group(1))
                except json.JSONDecodeError:
                    meta = {}
                continue
            if line.strip().startswith("--"):
                continue
            body_lines.append(line)
        sql = "\n".join(body_lines).strip().rstrip(";").strip()
        if path.name.startswith("ducklake"):
            sql = re.sub(r"\bdl\.", "", sql, flags=re.IGNORECASE)
        if not sql:
            continue
        queries.append(CorpusQuery(name=path.stem, sql=sql, meta=meta))
    return queries


# ---------------------------------------------------------------------------
# Translation
# ---------------------------------------------------------------------------

_TRANSLATE_MARKER = "__lpts_q__"
_CORPUS_FORMAT_VERSION = 4


class Translator:
    """Batch LPTS translator driven through the duckdb-openivm CLI.

    One CLI process handles a whole chunk: LPTS needs the base tables to exist
    (it plans each query before re-rendering it), so the session is set up once
    and reused. Chunking bounds the blast radius if a query kills the process.
    """

    def __init__(
        self,
        duckdb_bin: str,
        lpts_extension: str,
        setup_sql: Sequence[str],
        *,
        chunk_size: int = 250,
        timeout_s: float = 900.0,
    ) -> None:
        self._bin = duckdb_bin
        self._ext = lpts_extension
        self._setup = list(setup_sql)
        self._chunk_size = chunk_size
        self._timeout_s = timeout_s

    def _script(self, queries: Sequence[CorpusQuery], dialect: str) -> str:
        lines = [
            ".bail off",
            # A handful of corpus queries need icu (timezone/collation). Autoloading
            # it mid-statement fails when the extension is absent and the host is
            # offline, which would cost those queries their translation. Try up
            # front and let it fail harmlessly under `.bail off` if unavailable.
            _sql("duckdb-openivm", "lpts_install_icu"),
            _sql("duckdb-openivm", "lpts_load_icu"),
            _sql("duckdb-openivm", "lpts_load_extension", extension=self._ext),
            _sql("duckdb-openivm", "lpts_set_dialect", dialect=dialect),
            _sql("duckdb-openivm", "lpts_unqualified"),
        ]
        lines += [stmt.rstrip(";") + ";" for stmt in self._setup]
        lines.append(".mode json")
        for query in queries:
            escaped = query.sql.replace("'", "''")
            lines.append(
                _sql(
                    "duckdb-openivm", "lpts_marker",
                    marker=_TRANSLATE_MARKER + query.name,
                )
            )
            # A SELECT may legally expose the same name more than once, while
            # Spark tables and Feldera verifier subqueries require unique names.
            # DuckDB assigns stable positional suffixes at this subquery boundary
            # (a, a_1, ...); LPTS then preserves those aliases in every target.
            lines.append(_sql("duckdb-openivm", "lpts_translate", sql=escaped))
        return "\n".join(lines) + "\n"

    def _run_chunk(
        self, queries: Sequence[CorpusQuery], dialect: str
    ) -> Dict[str, Tuple[bool, str]]:
        try:
            proc = subprocess.run(
                [self._bin, "-unsigned"],
                input=self._script(queries, dialect),
                text=True,
                capture_output=True,
                timeout=self._timeout_s,
            )
        except subprocess.TimeoutExpired:
            return {q.name: (False, "translation timeout") for q in queries}

        results = self._parse(proc.stdout)
        errors = self._parse_errors(proc.stderr)
        out: Dict[str, Tuple[bool, str]] = {}
        error_idx = 0
        for query in queries:
            translated = results.get(query.name)
            if translated:
                out[query.name] = (True, translated)
            else:
                # stderr carries LPTS refusals in statement order; pair them with
                # the failures in the same order so each row keeps its reason.
                reason = errors[error_idx] if error_idx < len(errors) else "translation failed"
                error_idx += 1
                out[query.name] = (False, reason)
        return out

    @staticmethod
    def _parse(stdout: str) -> Dict[str, str]:
        """Pull `marker -> translated sql` pairs out of the CLI's JSON output.

        Each statement emits its own JSON array. A marker array names the query;
        the next `sql` array (if any) is its translation. A marker followed
        directly by another marker means that query produced no output — a
        failure, resolved by the caller against stderr.
        """
        out: Dict[str, str] = {}
        pending: Optional[str] = None
        for raw in stdout.splitlines():
            line = raw.strip()
            if not line.startswith("[") and not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows = payload if isinstance(payload, list) else [payload]
            for row in rows:
                if not isinstance(row, dict):
                    continue
                marker = row.get("marker")
                if isinstance(marker, str) and marker.startswith(_TRANSLATE_MARKER):
                    pending = marker[len(_TRANSLATE_MARKER):]
                    continue
                sql = row.get("sql")
                if isinstance(sql, str) and pending:
                    out[pending] = sql.strip()
                    pending = None
        return out

    @staticmethod
    def _parse_errors(stderr: str) -> List[str]:
        errors: List[str] = []
        for raw in stderr.splitlines():
            line = raw.strip()
            if not line:
                continue
            # DuckDB prints "<Kind> Error: <message>" then indented context.
            if re.match(r"^[A-Za-z ]+Error:", line):
                errors.append(line[:500])
        return errors

    def translate(
        self, queries: Sequence[CorpusQuery], dialect: str
    ) -> Dict[str, Tuple[bool, str]]:
        out: Dict[str, Tuple[bool, str]] = {}
        for start in range(0, len(queries), self._chunk_size):
            chunk = queries[start : start + self._chunk_size]
            out.update(self._run_chunk_resilient(chunk, dialect))
            logger.info(
                "[compiler-bench] translate %s: %d/%d queries",
                dialect,
                min(start + self._chunk_size, len(queries)),
                len(queries),
            )
        return out

    def _run_chunk_resilient(
        self, queries: Sequence[CorpusQuery], dialect: str
    ) -> Dict[str, Tuple[bool, str]]:
        """Retry halves when a CLI failure leaves queries unexplained."""
        result = self._run_chunk(queries, dialect)
        unexplained = any(
            not success and reason in ("translation failed", "translation timeout")
            for success, reason in result.values()
        )
        if not unexplained or len(queries) <= 1:
            return result

        # A process crash midway through a chunk otherwise marks every later
        # query as unsupported. Bisect until only the pathological query fails.
        midpoint = len(queries) // 2
        recovered = self._run_chunk_resilient(queries[:midpoint], dialect)
        recovered.update(self._run_chunk_resilient(queries[midpoint:], dialect))
        return recovered


# ---------------------------------------------------------------------------
# Preparation entry point
# ---------------------------------------------------------------------------


@dataclass
class CorpusPaths:
    root: Path
    duckdb_bin: Path
    lpts_extension: Path
    corpus_src: Path

    @classmethod
    def from_repo(cls, repo_dir: str) -> "CorpusPaths":
        mount = Path(repo_dir) / "mount"
        return cls(
            root=mount / "compiler-bench" / "corpus",
            duckdb_bin=mount / "bin" / "duckdb-openivm" / "duckdb",
            lpts_extension=mount / "bin" / "lpts" / "lpts.duckdb_extension",
            corpus_src=mount / "bin" / "duckdb-openivm" / "queries" / CORPUS_SUBDIR,
        )


def _cache_key(
    dialects: Sequence[str], queries: Sequence[CorpusQuery], lpts_sha: str
) -> str:
    digest = hashlib.sha256()
    digest.update(str(_CORPUS_FORMAT_VERSION).encode())
    digest.update(b"\0".join(d.encode() for d in sorted(dialects)))
    digest.update(lpts_sha.encode())
    for query in queries:
        digest.update(query.name.encode())
        digest.update(query.sql.encode())
    return digest.hexdigest()


def _read_translation_rows(dialect_dir: Path) -> List[dict]:
    path = dialect_dir / "translation.csv"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _lpts_sha(extension: Path) -> str:
    sums = extension.parent / "SHA256SUMS"
    if sums.exists():
        return sums.read_text(encoding="utf-8").split()[0]
    if extension.exists():
        return hashlib.sha256(extension.read_bytes()).hexdigest()
    return "missing"




def prepare(
    *,
    repo_dir: str,
    engines: Sequence[str],
    scale_factor: int = 3,
    limit: int = 0,
    force: bool = False,
) -> dict:
    """Translate the TPC-C corpus for every dialect ``engines`` need.

    Idempotent: a matching ``meta.json`` cache key short-circuits the whole
    step, so repeated experiments in a sweep pay translation once.
    """

    paths = CorpusPaths.from_repo(repo_dir)
    dialects = dialects_for(engines)
    if not dialects:
        raise ValueError(f"no known dialect for engines: {list(engines)}")

    if not paths.duckdb_bin.exists():
        raise RuntimeError(
            f"duckdb-openivm binary not found at {paths.duckdb_bin} — "
            "the duckdb-openivm build must run before compiler-bench prep"
        )
    needs_lpts = any(dialect != "duckdb" for dialect in dialects)
    if needs_lpts and not paths.lpts_extension.exists():
        raise RuntimeError(
            f"LPTS extension not found at {paths.lpts_extension} — "
            "the lpts build must run before compiler-bench prep"
        )
    if not paths.corpus_src.is_dir():
        raise RuntimeError(
            f"query corpus not found at {paths.corpus_src} — the duckdb-openivm "
            "image must be rebuilt to export benchmark/queries (see its entrypoint)"
        )

    queries = read_corpus(paths.corpus_src)
    if limit:
        queries = queries[:limit]
    if not queries:
        raise RuntimeError(f"no queries found in {paths.corpus_src}")

    lpts_sha = _lpts_sha(paths.lpts_extension) if needs_lpts else "not-used"
    key = _cache_key(dialects, queries, lpts_sha)
    meta_path = paths.root / "meta.json"
    if not force and meta_path.exists():
        try:
            cached = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            cached = {}
        if cached.get("cache_key") == key:
            logger.info(
                "[compiler-bench] corpus cache hit (%s): %d queries, dialects=%s",
                key[:12],
                cached.get("query_count"),
                ",".join(cached.get("dialects", {}).keys()),
            )
            cached["cached"] = True
            return cached

    setup_sql = tpcc_schema_ddl("duckdb-openivm")

    translator = (
        Translator(str(paths.duckdb_bin), str(paths.lpts_extension), setup_sql)
        if needs_lpts
        else None
    )

    paths.root.mkdir(parents=True, exist_ok=True)
    per_dialect: Dict[str, dict] = {}
    translatable: Dict[str, set] = {}

    for dialect in dialects:
        engine = next(
            name for name, target in ENGINE_DIALECTS.items()
            if target == dialect and name in engines
        )
        if dialect == "duckdb":
            # DuckDB and DuckDB-OpenIVM are the native source engines for this
            # corpus. Replanning and re-rendering their already-valid SQL via
            # LPTS can only lose coverage (and previously made one extreme
            # query crash the translator, falsely excluding its whole batch).
            results = {query.name: (True, query.sql) for query in queries}
        else:
            assert translator is not None
            results = translator.translate(queries, dialect)
        dialect_dir = paths.root / dialect
        queries_dir = dialect_dir / "queries"
        if queries_dir.exists():
            for stale in queries_dir.glob("*.sql"):
                stale.unlink()
        queries_dir.mkdir(parents=True, exist_ok=True)

        rows = []
        ok = 0
        for query in queries:
            success, payload = results.get(query.name, (False, "no result"))
            if success:
                (queries_dir / f"{query.name}.sql").write_text(payload + "\n", encoding="utf-8")
                ok += 1
            rows.append(
                {
                    "query_name": query.name,
                    "status": "translated" if success else "translation_failed",
                    "meta_is_incremental": (
                        "" if query.meta_is_incremental is None
                        else int(query.meta_is_incremental)
                    ),
                    "error": "" if success else payload,
                }
            )
        translatable[dialect] = {r["query_name"] for r in rows if r["status"] == "translated"}

        buf = io.StringIO()
        writer = csv.DictWriter(
            buf, fieldnames=["query_name", "status", "meta_is_incremental", "error"]
        )
        writer.writeheader()
        writer.writerows(rows)
        (dialect_dir / "translation.csv").write_text(buf.getvalue(), encoding="utf-8")

        (dialect_dir / "schema.sql").write_text(
            ";\n".join(tpcc_schema_ddl(engine)) + ";\n", encoding="utf-8"
        )
        (dialect_dir / "deltas.sql").write_text(
            ";\n".join(tpcc_delta_pool(scale_factor, engine)) + ";\n", encoding="utf-8"
        )

        per_dialect[dialect] = {
            "translated": ok,
            "translation_failed": len(queries) - ok,
            "pct_translated": round(100.0 * ok / len(queries), 2),
        }
        logger.info(
            "[compiler-bench] %s: %d/%d translated (%.1f%%)",
            dialect, ok, len(queries), per_dialect[dialect]["pct_translated"],
        )

    # A dialect that translated nothing means the transpiler is broken, not that
    # the corpus is inexpressible — e.g. an LPTS extension built against the
    # wrong DuckDB version fails every LOAD. Fail here: proceeding would start
    # engines against an empty corpus and report 0% incrementalizable, which
    # reads like an engine result.
    for dialect, names in translatable.items():
        if not names:
            sample = next(
                (
                    r["error"]
                    for r in _read_translation_rows(paths.root / dialect)
                    if r.get("error")
                ),
                "no error recorded",
            )
            raise RuntimeError(
                f"compiler-bench: translation produced 0 of {len(queries)} queries "
                f"for dialect {dialect!r} — the transpiler is not working. "
                f"First error: {sample}"
            )

    # Queries every dialect could express. Cross-engine percentages computed over
    # this set are comparable; per-engine percentages over each engine's own
    # corpus are not, because the dialects lose different queries.
    common = set.intersection(*translatable.values()) if translatable else set()
    (paths.root / "common.txt").write_text(
        "\n".join(sorted(common)) + "\n", encoding="utf-8"
    )

    meta = {
        "cache_key": key,
        "scale_factor": scale_factor,
        "query_count": len(queries),
        "common_count": len(common),
        "dialects": per_dialect,
        "engines": {e: ENGINE_DIALECTS[e] for e in engines if e in ENGINE_DIALECTS},
        "lpts_sha256": lpts_sha,
        "cached": False,
    }
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return meta


def generate_tpcc_parquet(
    *, repo_dir: str, scale_factor: int, force: bool = False
) -> dict:
    """Materialize the TPC-C base tables once, as Parquet, for engines to ingest.

    Generating in DuckDB and handing every engine the same files keeps their
    base data byte-identical — otherwise each engine would re-derive it from its
    own dialect's generator and small differences (timestamp resolution, decimal
    rounding) would show up as spurious verification failures.
    """
    mount = Path(repo_dir) / "mount"
    duckdb_bin = mount / "bin" / "duckdb-openivm" / "duckdb"
    out_dir = mount / "compiler-bench" / "data" / f"sf{scale_factor}"
    marker = out_dir / "_SUCCESS"

    if marker.exists() and not force:
        logger.info("[compiler-bench] TPC-C sf%d Parquet already present", scale_factor)
        return {"status": "cached", "path": str(out_dir), "scale_factor": scale_factor}

    if not duckdb_bin.exists():
        raise RuntimeError(f"duckdb-openivm binary not found at {duckdb_bin}")

    out_dir.mkdir(parents=True, exist_ok=True)
    stmts = [".bail on"]
    stmts += [s + ";" for s in tpcc_schema_ddl("duckdb-openivm")]
    stmts += [s + ";" for s in tpcc_load_sql_duckdb(scale_factor)]
    for table in TPCC_TABLES:
        target = str(out_dir / f"{table}.parquet").replace("'", "''")
        stmts.append(
            _sql("duckdb-openivm", "tpcc_copy_parquet", table=table, target=target)
        )
        # CSV as well: Feldera ingests over HTTP and takes CSV/JSON, not Parquet.
        # Same generated rows, so every engine still loads identical data.
        csv_target = str(out_dir / f"{table}.csv").replace("'", "''")
        stmts.append(
            _sql("duckdb-openivm", "tpcc_copy_csv", table=table, target=csv_target)
        )

    proc = subprocess.run(
        [str(duckdb_bin), "-unsigned"],
        input="\n".join(stmts) + "\n",
        text=True,
        capture_output=True,
        timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"TPC-C Parquet generation failed (exit {proc.returncode}):\n"
            f"{proc.stdout[-2000:]}\n{proc.stderr[-2000:]}"
        )

    missing = [t for t in TPCC_TABLES if not (out_dir / f"{t}.parquet").exists()]
    if missing:
        raise RuntimeError(f"TPC-C Parquet generation produced no output for: {missing}")

    marker.write_text(f"scale_factor={scale_factor}\n", encoding="utf-8")
    total_bytes = sum(
        (out_dir / f"{t}.parquet").stat().st_size for t in TPCC_TABLES
    )
    logger.info(
        "[compiler-bench] TPC-C sf%d Parquet written to %s (%.1f MiB)",
        scale_factor, out_dir, total_bytes / (1024 * 1024),
    )
    return {
        "status": "generated",
        "path": str(out_dir),
        "scale_factor": scale_factor,
        "bytes": total_bytes,
    }
