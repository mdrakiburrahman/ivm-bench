"""Read the prepared compiler-bench corpus from the shared mount.

The benchmark-server's `compiler_bench_corpus.prepare()` writes one translated
corpus per dialect; this side only consumes it. A query missing from a dialect's
corpus is reported as `translation_failed`, never skipped — skipping would
inflate the engine's success rate.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

CORPUS_ROOT = Path(os.environ.get("COMPILER_BENCH_CORPUS_DIR", "/data/compiler-bench/corpus"))

# Engine -> dialect. Must agree with benchmark-server's
# services/compiler_bench_corpus.ENGINE_DIALECTS; the runner asserts the dialect
# directory exists, so a divergence fails loudly at startup instead of running
# the wrong corpus.
ENGINE_DIALECTS: Dict[str, str] = {
    "duckdb": "duckdb",
    "duckdb-openivm": "duckdb",
    "spark": "spark",
    "spark-openivm": "spark",
    "databricks-enzyme": "spark",
    "fabric-jvm-35": "spark",
    "fabric-openivm-jvm-35": "spark",
    "feldera": "feldera",
    # RisingWave speaks PostgreSQL; the adapter strips the
    # length/precision type parameters its parser rejects.
    "risingwave": "postgres",
}


@dataclass
class Query:
    name: str
    sql: str
    """Translated SQL, or empty when translation failed for this dialect."""
    translated: bool
    translation_error: str = ""
    meta_is_incremental: Optional[bool] = None
    #: True when every dialect could express this query — the only subset over
    #: which cross-engine percentages are comparable.
    in_common: bool = False


@dataclass
class Corpus:
    dialect: str
    engine: str
    queries: List[Query] = field(default_factory=list)
    schema_ddl: List[str] = field(default_factory=list)
    deltas: List[str] = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    @property
    def translated(self) -> List[Query]:
        return [q for q in self.queries if q.translated]


def _split_statements(text: str) -> List[str]:
    """Split a `;`-terminated DDL/DML script into statements.

    The generated schema/delta scripts are one statement per line with no
    embedded semicolons, so a newline-aware split is sufficient and avoids
    dragging in a SQL parser.
    """
    out: List[str] = []
    for chunk in text.split(";\n"):
        stmt = chunk.strip().rstrip(";").strip()
        if stmt:
            out.append(stmt)
    return out


def load(engine: str, *, limit: int = 0, root: Optional[Path] = None) -> Corpus:
    """Load the prepared corpus for ``engine``."""
    dialect = ENGINE_DIALECTS.get(engine)
    if not dialect:
        raise ValueError(f"no compiler-bench dialect registered for engine {engine!r}")

    base = root or CORPUS_ROOT
    dialect_dir = base / dialect
    if not dialect_dir.is_dir():
        raise RuntimeError(
            f"prepared corpus not found at {dialect_dir} — the benchmark-server's "
            "compiler-bench prep step must run before the engine run"
        )

    meta: dict = {}
    meta_path = base / "meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("[compiler-bench] unreadable meta.json at %s", meta_path)

    common: set = set()
    common_path = base / "common.txt"
    if common_path.exists():
        common = {
            line.strip()
            for line in common_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    queries: List[Query] = []
    translation_csv = dialect_dir / "translation.csv"
    if not translation_csv.exists():
        raise RuntimeError(f"translation.csv missing from {dialect_dir}")

    with translation_csv.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            name = row["query_name"]
            translated = row.get("status") == "translated"
            sql = ""
            if translated:
                sql_path = dialect_dir / "queries" / f"{name}.sql"
                if sql_path.exists():
                    sql = sql_path.read_text(encoding="utf-8").strip().rstrip(";").strip()
                else:
                    # translation.csv says translated but the file is gone: treat
                    # as a translation failure rather than running empty SQL.
                    translated = False
                    row["error"] = f"translated SQL missing at {sql_path}"
            raw_meta = (row.get("meta_is_incremental") or "").strip()
            queries.append(
                Query(
                    name=name,
                    sql=sql,
                    translated=translated,
                    translation_error="" if translated else (row.get("error") or ""),
                    meta_is_incremental=bool(int(raw_meta)) if raw_meta else None,
                    in_common=name in common,
                )
            )

    if limit:
        queries = queries[:limit]

    schema_path = dialect_dir / "schema.sql"
    deltas_path = dialect_dir / "deltas.sql"
    return Corpus(
        dialect=dialect,
        engine=engine,
        queries=queries,
        schema_ddl=(
            _split_statements(schema_path.read_text(encoding="utf-8"))
            if schema_path.exists() else []
        ),
        deltas=(
            _split_statements(deltas_path.read_text(encoding="utf-8"))
            if deltas_path.exists() else []
        ),
        meta=meta,
    )
