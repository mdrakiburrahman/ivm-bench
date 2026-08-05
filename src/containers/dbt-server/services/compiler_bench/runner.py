"""compiler-bench phase machine.

Each query is pushed as far as the engine will take it, and the phase it stops
at is the result:

  1 base query -> 2 MV creation -> 2b classification -> 3 deltas
  -> 4 refresh -> 5 verify -> 6 ok

Phase codes match openivm's `benchmark/src/rewriter_benchmark.cpp` so runs are
comparable with the original C++ benchmark; codes this harness adds sit above
them (97) or reuse its out-of-band values (98 timeout / 99 crash).

Classification is recorded only when the engine reports one — an engine we could
not interrogate is `unknown`, never `full`. `is_correct` is likewise only set
when verification actually ran.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, List, Optional

from services.compiler_bench.corpus import Corpus, Query
from services.compiler_bench.engines import (
    EngineAdapter,
    EngineCrashed,
    EngineTimeout,
    QueryFailed,
)

logger = logging.getLogger(__name__)

# --- phase codes (1-6 and 98/99 mirror the C++ benchmark) -------------------
PHASE_NOT_STARTED = 0
PHASE_BASE_QUERY_FAILED = 1
PHASE_MV_CREATION_FAILED = 2
PHASE_DELTA_FAILED = 3
PHASE_REFRESH_FAILED = 4
PHASE_VERIFY_FAILED = 5
PHASE_OK = 6
PHASE_TRANSLATION_FAILED = 97
PHASE_TIMEOUT = 98
PHASE_CRASH = 99

PHASE_NAMES = {
    PHASE_NOT_STARTED: "not_started",
    PHASE_BASE_QUERY_FAILED: "base_query_failed",
    PHASE_MV_CREATION_FAILED: "mv_creation_failed",
    PHASE_DELTA_FAILED: "delta_failed",
    PHASE_REFRESH_FAILED: "refresh_failed",
    PHASE_VERIFY_FAILED: "verify_failed",
    PHASE_OK: "ok",
    PHASE_TRANSLATION_FAILED: "translation_failed",
    PHASE_TIMEOUT: "timeout",
    PHASE_CRASH: "crash",
}

CLASSIFICATION_INCREMENTAL = "incremental"
CLASSIFICATION_FULL = "full"
CLASSIFICATION_UNKNOWN = "unknown"


@dataclass
class QueryResult:
    query_name: str
    engine: str
    dialect: str
    phase_reached: int
    phase_name: str
    classification: str
    meta_is_incremental: Optional[bool]
    is_correct: Optional[bool]
    in_common: bool
    # Recorded when the phase actually succeeded, not inferred from
    # phase_reached: a crash during a later phase overwrites the code with 99
    # and would otherwise erase the fact that the view was created.
    mv_created: bool = False
    refresh_ok: bool = False
    time_base_query_ms: float = 0.0
    time_mv_ms: float = 0.0
    time_refresh_ms: float = 0.0
    time_verify_ms: float = 0.0
    error: str = ""


CSV_COLUMNS = [
    "query_name", "engine", "dialect", "phase_reached", "phase_name",
    "classification", "meta_is_incremental", "actual_is_incremental",
    "is_correct", "in_common", "mv_created", "refresh_ok",
    "time_base_query_ms", "time_mv_ms", "time_refresh_ms", "time_verify_ms",
    "error",
]


def _csv_bool(value: Optional[bool]) -> str:
    """Render a tri-state as CSV. Empty means "not determined" — distinct from 0."""
    if value is None:
        return ""
    return "1" if value else "0"


def result_to_row(result: QueryResult) -> Dict[str, object]:
    actual = None
    if result.classification == CLASSIFICATION_INCREMENTAL:
        actual = True
    elif result.classification == CLASSIFICATION_FULL:
        actual = False
    return {
        "query_name": result.query_name,
        "engine": result.engine,
        "dialect": result.dialect,
        "phase_reached": result.phase_reached,
        "phase_name": result.phase_name,
        "classification": result.classification,
        "meta_is_incremental": _csv_bool(result.meta_is_incremental),
        "actual_is_incremental": _csv_bool(actual),
        "is_correct": _csv_bool(result.is_correct),
        "in_common": _csv_bool(result.in_common),
        "mv_created": _csv_bool(result.mv_created),
        "refresh_ok": _csv_bool(result.refresh_ok),
        "time_base_query_ms": round(result.time_base_query_ms, 4),
        "time_mv_ms": round(result.time_mv_ms, 4),
        "time_refresh_ms": round(result.time_refresh_ms, 4),
        "time_verify_ms": round(result.time_verify_ms, 4),
        "error": result.error[:2000],
    }


class CompilerBenchRunner:
    """Drive one engine through a corpus."""

    def __init__(
        self,
        adapter: EngineAdapter,
        corpus: Corpus,
        *,
        timeout_s: float = 60.0,
        delta_batch_size: int = 10,
        verify: bool = True,
        progress: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self._adapter = adapter
        self._corpus = corpus
        self._timeout_s = timeout_s
        self._delta_batch_size = delta_batch_size
        self._verify = verify
        self._progress = progress
        self._delta_idx = 0
        self.results: List[QueryResult] = []

    # ----- delta pool ------------------------------------------------------

    def _next_delta_batch(self) -> List[str]:
        """Next slice of the delta pool, wrapping so every query sees churn."""
        deltas = self._corpus.deltas
        if not deltas:
            return []
        batch = []
        for _ in range(self._delta_batch_size):
            batch.append(deltas[self._delta_idx])
            self._delta_idx = (self._delta_idx + 1) % len(deltas)
        return batch

    # ----- per-query -------------------------------------------------------

    def _run_query(self, query: Query, mv_name: str) -> QueryResult:
        result = QueryResult(
            query_name=query.name,
            engine=self._corpus.engine,
            dialect=self._corpus.dialect,
            phase_reached=PHASE_NOT_STARTED,
            phase_name=PHASE_NAMES[PHASE_NOT_STARTED],
            classification=CLASSIFICATION_UNKNOWN,
            meta_is_incremental=query.meta_is_incremental,
            is_correct=None,
            in_common=query.in_common,
        )

        if not query.translated:
            result.phase_reached = PHASE_TRANSLATION_FAILED
            result.phase_name = PHASE_NAMES[PHASE_TRANSLATION_FAILED]
            result.error = query.translation_error
            return result

        deadline = time.monotonic() + self._timeout_s

        def remaining() -> float:
            left = deadline - time.monotonic()
            if left <= 0:
                raise EngineTimeout(f"query budget of {self._timeout_s:.0f}s exhausted")
            return left

        # phase_reached always holds the failure code of the phase in flight, so
        # whatever raises leaves the right code behind without extra bookkeeping.
        try:
            # Phase 1 — can the engine run the SELECT at all?
            result.phase_reached = PHASE_BASE_QUERY_FAILED
            start = time.monotonic()
            self._adapter.run_base_query(query.sql, timeout_s=remaining())
            result.time_base_query_ms = (time.monotonic() - start) * 1000.0

            # Phase 2 — MV creation.
            result.phase_reached = PHASE_MV_CREATION_FAILED
            start = time.monotonic()
            self._adapter.create_mv(mv_name, query.sql, timeout_s=remaining())
            result.time_mv_ms = (time.monotonic() - start) * 1000.0
            result.mv_created = True

            # Phase 2b — classification. A failure to *ask* is not a failure of
            # the query: keep going and leave the verdict `unknown`.
            try:
                result.classification = self._adapter.classify(
                    mv_name, query.sql, timeout_s=remaining()
                )
            except (EngineTimeout, EngineCrashed):
                raise
            except Exception as exc:
                result.classification = CLASSIFICATION_UNKNOWN
                logger.debug(
                    "[compiler-bench] %s: classification failed for %s: %s",
                    self._corpus.engine, query.name, exc,
                )

            # Phase 3 — base-table deltas.
            result.phase_reached = PHASE_DELTA_FAILED
            self._adapter.apply_deltas(self._next_delta_batch(), timeout_s=remaining())

            # Phase 4 — refresh.
            result.phase_reached = PHASE_REFRESH_FAILED
            start = time.monotonic()
            self._adapter.refresh(mv_name, query.sql, timeout_s=remaining())
            result.time_refresh_ms = (time.monotonic() - start) * 1000.0
            result.refresh_ok = True

            # What the engine actually did beats what it predicted.
            try:
                observed = self._adapter.observed_classification(
                    mv_name, timeout_s=remaining()
                )
            except (EngineTimeout, EngineCrashed):
                raise
            except Exception:
                observed = CLASSIFICATION_UNKNOWN
            if observed != CLASSIFICATION_UNKNOWN:
                result.classification = observed

            # Phase 5 — verification. Only claim correctness when it really ran.
            if self._verify and self._adapter.supports_verify:
                result.phase_reached = PHASE_VERIFY_FAILED
                start = time.monotonic()
                correct = self._adapter.verify(mv_name, query.sql, timeout_s=remaining())
                result.time_verify_ms = (time.monotonic() - start) * 1000.0
                result.is_correct = correct
                result.phase_reached = PHASE_OK if correct else PHASE_VERIFY_FAILED
                if not correct:
                    result.error = result.error or "MV contents differ from the base query"
            else:
                result.is_correct = None
                result.phase_reached = PHASE_OK

        except EngineTimeout as exc:
            result.phase_reached = PHASE_TIMEOUT
            result.error = str(exc)
        except EngineCrashed as exc:
            result.phase_reached = PHASE_CRASH
            result.error = str(exc)
        except QueryFailed as exc:
            # The phase counter already holds the phase that was in flight.
            result.error = str(exc)
        except Exception as exc:  # defensive: an adapter bug must not kill the run
            result.phase_reached = PHASE_CRASH
            result.error = f"harness error: {exc}"
            logger.exception(
                "[compiler-bench] %s: unexpected error on %s",
                self._corpus.engine, query.name,
            )

        result.phase_name = PHASE_NAMES.get(result.phase_reached, "unknown")
        return result

    # ----- run loop --------------------------------------------------------

    def run(self) -> dict:
        adapter = self._adapter
        adapter.setup(self._corpus)

        total = len(self._corpus.queries)
        for index, query in enumerate(self._corpus.queries, start=1):
            mv_name = f"cb_mv_{index}"
            result = self._run_query(query, mv_name)
            self.results.append(result)

            # Always try to clean up: thousands of live MVs slow every engine's
            # catalog down, and on Databricks each one is a separate pipeline.
            try:
                adapter.drop_mv(mv_name)
            except Exception:
                logger.debug(
                    "[compiler-bench] %s: drop of %s failed", self._corpus.engine, mv_name,
                    exc_info=True,
                )

            if result.phase_reached in (PHASE_CRASH, PHASE_TIMEOUT):
                # A dead session would turn every later query into a phantom
                # crash, so re-establish before continuing.
                try:
                    adapter.reset()
                except Exception:
                    logger.warning(
                        "[compiler-bench] %s: reset after %s failed",
                        self._corpus.engine, result.phase_name, exc_info=True,
                    )

            if self._progress and (index % 25 == 0 or index == total):
                self._progress({"completed": index, "total": total})
            if index % 100 == 0 or index == total:
                logger.info(
                    "[compiler-bench] %s: %d/%d queries", self._corpus.engine, index, total
                )

        try:
            adapter.teardown()
        except Exception:
            logger.warning(
                "[compiler-bench] %s: teardown failed", self._corpus.engine, exc_info=True
            )
        return self.summarize()

    # ----- summary ---------------------------------------------------------

    def summarize(self) -> dict:
        return summarize(self.results, engine=self._corpus.engine, corpus=self._corpus)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(100.0 * numerator / denominator, 2)


def summarize(
    results: List[QueryResult], *, engine: str, corpus: Optional[Corpus] = None
) -> dict:
    """Aggregate per-query results.

    Every percentage names its denominator, because the choice changes the
    story: pct_of_attempted divides by the whole corpus, pct_of_mv_created only
    by queries the engine accepted as a view.
    """
    total = len(results)
    by_phase: Dict[str, int] = {}
    for result in results:
        by_phase[result.phase_name] = by_phase.get(result.phase_name, 0) + 1

    translation_failed = by_phase.get("translation_failed", 0)
    crashed = by_phase.get("crash", 0)
    timed_out = by_phase.get("timeout", 0)
    attempted = total - translation_failed

    mv_created = sum(1 for r in results if r.mv_created)
    incremental = sum(1 for r in results if r.classification == CLASSIFICATION_INCREMENTAL)
    full = sum(1 for r in results if r.classification == CLASSIFICATION_FULL)
    unknown_class = mv_created - incremental - full

    refresh_ok = sum(1 for r in results if r.refresh_ok)
    verified = [r for r in results if r.is_correct is not None]
    correct = sum(1 for r in verified if r.is_correct)

    # Comparable slice: only over this subset are cross-engine percentages
    # meaningful, since dialects lose different queries to translation failure.
    common = [r for r in results if r.in_common]
    common_incremental = sum(
        1 for r in common if r.classification == CLASSIFICATION_INCREMENTAL
    )
    common_full = sum(1 for r in common if r.classification == CLASSIFICATION_FULL)

    summary = {
        "engine": engine,
        "dialect": corpus.dialect if corpus else None,
        "totals": {
            "corpus": total,
            "attempted": attempted,
            "translation_failed": translation_failed,
            "mv_created": mv_created,
            "incremental": incremental,
            "full_refresh": full,
            "classification_unknown": unknown_class,
            "refresh_ok": refresh_ok,
            "verified": len(verified),
            "correct": correct,
            "crashed": crashed,
            "timeout": timed_out,
        },
        "pct_of_attempted": {
            "mv_created": _pct(mv_created, attempted),
            "crashed": _pct(crashed, attempted),
            "timeout": _pct(timed_out, attempted),
            "incremental": _pct(incremental, attempted),
            "full_refresh": _pct(full, attempted),
        },
        "pct_of_mv_created": {
            "incremental": _pct(incremental, mv_created),
            "full_refresh": _pct(full, mv_created),
            "classification_unknown": _pct(unknown_class, mv_created),
            "refresh_ok": _pct(refresh_ok, mv_created),
        },
        "pct_of_corpus": {
            "translation_failed": _pct(translation_failed, total),
        },
        "pct_of_verified": {
            "correct": _pct(correct, len(verified)),
        },
        "common_subset": {
            "size": len(common),
            "incremental": common_incremental,
            "full_refresh": common_full,
            "pct_incremental": _pct(common_incremental, len(common)),
            "pct_full_refresh": _pct(common_full, len(common)),
        },
        "by_phase": by_phase,
        "metadata_confusion": _metadata_confusion(results),
    }
    return summary


def _metadata_confusion(results: List[QueryResult]) -> dict:
    """Corpus `is_incremental` prediction vs the engine's actual verdict."""
    counts = {"meta_true_actual_true": 0, "meta_true_actual_false": 0,
              "meta_false_actual_true": 0, "meta_false_actual_false": 0,
              "meta_missing": 0}
    for result in results:
        if result.classification not in (CLASSIFICATION_INCREMENTAL, CLASSIFICATION_FULL):
            continue
        if result.meta_is_incremental is None:
            counts["meta_missing"] += 1
            continue
        actual = result.classification == CLASSIFICATION_INCREMENTAL
        key = f"meta_{str(result.meta_is_incremental).lower()}_actual_{str(actual).lower()}"
        counts[key] += 1
    scored = (counts["meta_true_actual_true"] + counts["meta_true_actual_false"]
              + counts["meta_false_actual_true"] + counts["meta_false_actual_false"])
    agree = counts["meta_true_actual_true"] + counts["meta_false_actual_false"]
    counts["scored"] = scored
    counts["pct_agreement"] = _pct(agree, scored)
    return counts
