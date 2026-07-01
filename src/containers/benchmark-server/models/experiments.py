"""ExperimentInputs — every knob that varies per-experiment in an OAT sweep.

The OAT (one-at-a-time) harness lets benchmark.sh hand the server a JSON file
listing N partial-override experiments to run serially. This is the ONLY
benchmark entrypoint; there is no env-var single-experiment fallback.

Knob groups:
  * scale_factor / batch_*_pct / engines / parallel — what the existing
    BenchmarkConfig already understands
  * feature_flags — OPENIVM_VALIDATE, OPENIVM_PROFILE_REFRESH, OPENIVM_QUERY_LOG,
    PRESERVE_RAW (PRESERVE_RAW only governs whether ``mount/bin/`` survives
    across sweeps — per-SF ``mount/raw/<sf>/`` is always wiped between
    experiments by the disk-aware cleanup)
  * spark_tunables — driver_pct_ram, executor_pct_ram, shuffle_partitions,
    default_parallelism. Applied to BOTH spark and spark-openivm engines via the
    SPARK_*_RAM / SPARK_SUBMIT_* env vars that the entrypoints read first.

JSON file format::

    {
      "_design_notes": ["..."],
      "experiments": [
        { "label": "sf=3 baseline", "scale_factor": 3, ... },
        { "label": "sf=5", "scale_factor": 5 },
        ...
      ]
    }

Every field in every dataclass has a default; an experiment dict only needs
to override the knobs that vary. Unspecified knobs inherit the baseline.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Feature flags (boolean-ish env vars on benchmark.sh today)
# ---------------------------------------------------------------------------

@dataclass
class FeatureFlags:
    openivm_validate: bool = True
    openivm_profile_refresh: bool = True
    openivm_query_log: bool = True
    preserve_raw: bool = False  # no-op inside OAT mode (per-experiment cleanup)
    spark_metrics_capture: bool = True
    openivm_extra_conf: str = ""

    def to_compose_env(self) -> Dict[str, str]:
        return {
            "OPENIVM_VALIDATE": "1" if self.openivm_validate else "0",
            "OPENIVM_PROFILE_REFRESH": "1" if self.openivm_profile_refresh else "0",
            "OPENIVM_QUERY_LOG": "1" if self.openivm_query_log else "0",
            "PRESERVE_RAW": "1" if self.preserve_raw else "0",
            "SPARK_METRICS_CAPTURE": "1" if self.spark_metrics_capture else "0",
            "OPENIVM_EXTRA_CONF": self.openivm_extra_conf,
        }


# ---------------------------------------------------------------------------
# Spark tunables (forwarded to both spark and spark-openivm services)
# ---------------------------------------------------------------------------

@dataclass
class SparkTunables:
    """Tunables that the spark / spark-openivm entrypoints honour from env.

    The entrypoint reads SPARK_DRIVER_PCT_RAM / SPARK_EXECUTOR_PCT_RAM /
    SPARK_SUBMIT_SHUFFLE_PARTITIONS / SPARK_SUBMIT_DEFAULT_PARALLELISM
    BEFORE falling back to spark-defaults-breakdown.yaml. None/empty means
    "fall back to YAML"; we only emit env vars when the experiment actually
    pins a value, so unset experiments inherit the existing YAML defaults.

    ``dbt_threads`` is read by the spark / spark-openivm dbt profiles.yml
    (via ``SPARK_DBT_THREADS``) and caps how many models dbt builds
    concurrently. Because Livy runs ``local[*]`` (one JVM), a high thread
    count lets several heavy gold facts build at once and can OOM the
    single driver heap at large SF — capping threads is the lever to keep
    peak concurrent memory in check. None means "inherit the profiles.yml
    default" (30).
    """
    driver_pct_ram: Optional[int] = None
    executor_pct_ram: Optional[int] = None
    shuffle_partitions: Optional[int] = None
    default_parallelism: Optional[int] = None
    dbt_threads: Optional[int] = None

    def to_compose_env(self) -> Dict[str, str]:
        env: Dict[str, str] = {}
        if self.driver_pct_ram is not None:
            env["SPARK_DRIVER_PCT_RAM"] = str(self.driver_pct_ram)
        if self.executor_pct_ram is not None:
            env["SPARK_EXECUTOR_PCT_RAM"] = str(self.executor_pct_ram)
        if self.shuffle_partitions is not None:
            env["SPARK_SUBMIT_SHUFFLE_PARTITIONS"] = str(self.shuffle_partitions)
        if self.default_parallelism is not None:
            env["SPARK_SUBMIT_DEFAULT_PARALLELISM"] = str(self.default_parallelism)
        if self.dbt_threads is not None:
            env["SPARK_DBT_THREADS"] = str(self.dbt_threads)
        return env


# ---------------------------------------------------------------------------
# Top-level experiment inputs
# ---------------------------------------------------------------------------

@dataclass
class ExperimentInputs:
    scale_factor: int = 3
    batch_1_pct: str = "100"
    batch_2_pct: str = "1"
    batch_3_pct: str = "2"
    batch_2_update_pct: str = "0"
    batch_2_delete_pct: str = "0"
    batch_3_update_pct: str = "0"
    batch_3_delete_pct: str = "0"
    engines: List[str] = field(default_factory=lambda: ["spark", "spark-openivm"])
    parallel: bool = False
    feature_flags: FeatureFlags = field(default_factory=FeatureFlags)
    spark_tunables: SparkTunables = field(default_factory=SparkTunables)
    label: Optional[str] = None

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_compose_env(self) -> Dict[str, str]:
        """Project this experiment to the env-var map that compose / orchestrator consume."""
        env: Dict[str, str] = {
            "SCALE_FACTOR": str(self.scale_factor),
            "BATCH_1_PCT": str(self.batch_1_pct),
            "BATCH_2_PCT": str(self.batch_2_pct),
            "BATCH_3_PCT": str(self.batch_3_pct),
            "BATCH_2_UPDATE_PCT": str(self.batch_2_update_pct),
            "BATCH_2_DELETE_PCT": str(self.batch_2_delete_pct),
            "BATCH_3_UPDATE_PCT": str(self.batch_3_update_pct),
            "BATCH_3_DELETE_PCT": str(self.batch_3_delete_pct),
            "PARALLEL": "1" if self.parallel else "0",
            "ENGINES": ",".join(self.engines),
        }
        env.update(self.feature_flags.to_compose_env())
        env.update(self.spark_tunables.to_compose_env())
        return env

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "scale_factor": self.scale_factor,
            "batch_1_pct": self.batch_1_pct,
            "batch_2_pct": self.batch_2_pct,
            "batch_3_pct": self.batch_3_pct,
            "batch_2_update_pct": self.batch_2_update_pct,
            "batch_2_delete_pct": self.batch_2_delete_pct,
            "batch_3_update_pct": self.batch_3_update_pct,
            "batch_3_delete_pct": self.batch_3_delete_pct,
            "engines": list(self.engines),
            "parallel": self.parallel,
            "feature_flags": asdict(self.feature_flags),
            "spark_tunables": asdict(self.spark_tunables),
        }

    def flat_values(self) -> Dict[str, Any]:
        """Flatten to (col_key -> value) for chart rendering."""
        out: Dict[str, Any] = {
            "scale_factor": self.scale_factor,
            "batch_1_pct": self.batch_1_pct,
            "batch_2_pct": self.batch_2_pct,
            "batch_3_pct": self.batch_3_pct,
            "batch_2_update_pct": self.batch_2_update_pct,
            "batch_2_delete_pct": self.batch_2_delete_pct,
            "batch_3_update_pct": self.batch_3_update_pct,
            "batch_3_delete_pct": self.batch_3_delete_pct,
            "engines": ",".join(self.engines),
            "parallel": int(self.parallel),
        }
        for f in fields(FeatureFlags):
            v = getattr(self.feature_flags, f.name)
            out[f"feature_flags.{f.name}"] = int(v) if isinstance(v, bool) else v
        for f in fields(SparkTunables):
            v = getattr(self.spark_tunables, f.name)
            out[f"spark_tunables.{f.name}"] = "" if v is None else v
        return out

    @classmethod
    def column_headers(cls) -> Dict[str, str]:
        """Stable (col_key -> human header) for chart rendering."""
        headers = {
            "scale_factor": "SF",
            "batch_1_pct": "b1%",
            "batch_2_pct": "b2%",
            "batch_3_pct": "b3%",
            "batch_2_update_pct": "b2u%",
            "batch_2_delete_pct": "b2d%",
            "batch_3_update_pct": "b3u%",
            "batch_3_delete_pct": "b3d%",
            "engines": "engines",
            "parallel": "parallel",
        }
        for f in fields(FeatureFlags):
            headers[f"feature_flags.{f.name}"] = f.name
        for f in fields(SparkTunables):
            headers[f"spark_tunables.{f.name}"] = f.name
        return headers

    @classmethod
    def from_dict(cls, d: Dict[str, Any], baseline: Optional["ExperimentInputs"] = None) -> "ExperimentInputs":
        """Build from a partial dict. Unspecified keys inherit ``baseline`` (or class defaults)."""
        base = baseline or cls()

        # Engines accept list-of-strings, "comma,separated" string, or absent.
        engines_raw = d.get("engines")
        if engines_raw is None:
            engines = list(base.engines)
        elif isinstance(engines_raw, str):
            engines = [e.strip() for e in engines_raw.split(",") if e.strip()]
        else:
            engines = list(engines_raw)

        ff_d = d.get("feature_flags") or {}
        ff = FeatureFlags(
            openivm_validate=bool(ff_d.get("openivm_validate", base.feature_flags.openivm_validate)),
            openivm_profile_refresh=bool(ff_d.get("openivm_profile_refresh", base.feature_flags.openivm_profile_refresh)),
            openivm_query_log=bool(ff_d.get("openivm_query_log", base.feature_flags.openivm_query_log)),
            preserve_raw=bool(ff_d.get("preserve_raw", base.feature_flags.preserve_raw)),
            spark_metrics_capture=bool(ff_d.get("spark_metrics_capture", base.feature_flags.spark_metrics_capture)),
            openivm_extra_conf=str(ff_d.get("openivm_extra_conf", base.feature_flags.openivm_extra_conf)),
        )

        st_d = d.get("spark_tunables") or {}
        st = SparkTunables(
            driver_pct_ram=st_d.get("driver_pct_ram", base.spark_tunables.driver_pct_ram),
            executor_pct_ram=st_d.get("executor_pct_ram", base.spark_tunables.executor_pct_ram),
            shuffle_partitions=st_d.get("shuffle_partitions", base.spark_tunables.shuffle_partitions),
            default_parallelism=st_d.get("default_parallelism", base.spark_tunables.default_parallelism),
            dbt_threads=st_d.get("dbt_threads", base.spark_tunables.dbt_threads),
        )

        # batch_N_pct accepts either the new explicit name or the legacy
        # batch_N_insert_pct alias. The OAT JSON canonicalises to batch_N_pct.
        def _pct(key: str, base_v: str) -> str:
            return str(d.get(key, d.get(key.replace("_pct", "_insert_pct"), base_v)))

        return cls(
            scale_factor=int(d.get("scale_factor", base.scale_factor)),
            batch_1_pct=_pct("batch_1_pct", base.batch_1_pct),
            batch_2_pct=_pct("batch_2_pct", base.batch_2_pct),
            batch_3_pct=_pct("batch_3_pct", base.batch_3_pct),
            batch_2_update_pct=str(d.get("batch_2_update_pct", base.batch_2_update_pct)),
            batch_2_delete_pct=str(d.get("batch_2_delete_pct", base.batch_2_delete_pct)),
            batch_3_update_pct=str(d.get("batch_3_update_pct", base.batch_3_update_pct)),
            batch_3_delete_pct=str(d.get("batch_3_delete_pct", base.batch_3_delete_pct)),
            engines=engines,
            parallel=bool(d.get("parallel", base.parallel)),
            feature_flags=ff,
            spark_tunables=st,
            label=d.get("label"),
        )


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_experiments_json(text: str) -> List[ExperimentInputs]:
    """Parse an OAT experiments JSON file.

    Top-level shape::

        {
          "baseline":    { ... optional overrides applied to ALL experiments ... },
          "experiments": [ { partial overrides }, { partial overrides }, ... ]
        }

    The ``baseline`` block is optional. If present, it's the parent each
    per-experiment row inherits from; otherwise each row inherits from
    ``ExperimentInputs()`` defaults.
    """
    data = json.loads(text)
    raw_exps = data.get("experiments") or []
    if not raw_exps:
        raise ValueError("experiments JSON has no 'experiments' array")

    baseline_d = data.get("baseline") or {}
    baseline = ExperimentInputs.from_dict(baseline_d) if baseline_d else ExperimentInputs()

    return [ExperimentInputs.from_dict(e, baseline=baseline) for e in raw_exps]


def _opt_int(key: str) -> Optional[int]:
    v = os.environ.get(key, "").strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None
