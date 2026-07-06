"""Result models for benchmark runs."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class BatchResult:
    """Result of a single batch run for an engine."""
    batch_num: int
    duration_s: float = 0.0
    status: str = "pending"  # pending, running, completed, failed
    error: Optional[str] = None
    # Forensics-only extras (e.g. databricks-enzyme pure-compute breakdown).
    # Chart consumers ignore this; it's serialized into benchmark-results.json
    # so reviewers can audit the swap between wall-clock and pure-compute.
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EngineResult:
    """Result of all batches for a single engine."""
    engine: str
    batches: List[BatchResult] = field(default_factory=list)
    status: str = "pending"  # pending, running, completed, failed
    error: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.batches:
            self.batches = [BatchResult(batch_num=i) for i in range(1, 4)]

    @property
    def total_duration_s(self) -> float:
        return sum(b.duration_s for b in self.batches)

    def to_dict(self) -> dict:
        d = {
            "engine": self.engine,
            "status": self.status,
            "error": self.error,
            "total_duration_s": self.total_duration_s,
            "batches": [
                {
                    "batch_num": b.batch_num,
                    "duration_s": b.duration_s,
                    "status": b.status,
                    "error": b.error,
                    **({"extra": b.extra} if b.extra else {}),
                }
                for b in self.batches
            ],
        }
        if self.extra:
            d["extra"] = self.extra
        return d


@dataclass
class BenchmarkResult:
    """Overall benchmark result."""
    status: str = "pending"  # pending, running, completed, failed
    engines: Dict[str, EngineResult] = field(default_factory=dict)
    repetitions: List[Dict[str, EngineResult]] = field(default_factory=list)
    repetition_count: int = 1
    total_duration_s: float = 0.0
    wall_clock_duration_s: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = {
            "status": self.status,
            "repetition_count": self.repetition_count,
            "total_duration_s": self.total_duration_s,
            "wall_clock_duration_s": self.wall_clock_duration_s,
            "error": self.error,
            "engines": {name: er.to_dict() for name, er in self.engines.items()},
        }
        if self.repetitions:
            d["repetitions"] = [
                {name: er.to_dict() for name, er in engines.items()}
                for engines in self.repetitions
            ]
        return d

    def summary_table(self) -> str:
        """Format the same summary table as the original benchmark.sh."""

        def fmt(secs: float) -> str:
            s = int(secs)
            return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"

        title = "=== All benchmarks completed successfully ==="
        if self.repetition_count > 1:
            title = f"=== Average benchmark results over {self.repetition_count} runs ==="

        max_label = max((len(name) + 1 for name in self.engines), default=10)
        label_width = max(12, max_label + 2)

        batch_nums = sorted({
            b.batch_num
            for er in self.engines.values()
            for b in er.batches
        })

        lines = ["", title, ""]
        lines.append(f"{'':{label_width}s}" + "".join(f"{bn:<12d}" for bn in batch_nums).rstrip())
        total = 0.0
        for name, er in self.engines.items():
            label = f"{name.capitalize()}:"
            durations = " -> ".join(fmt(b.duration_s) for b in er.batches)
            lines.append(f"{label:{label_width}s}{durations}")
            total += er.total_duration_s
        lines.append("")
        lines.append(f"================= {fmt(total)} ==================")
        return "\n".join(lines)
