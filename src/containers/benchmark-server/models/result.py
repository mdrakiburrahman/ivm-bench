"""Result models for benchmark runs."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class BatchResult:
    """Result of a single batch run for an engine."""
    batch_num: int
    duration_s: float = 0.0
    status: str = "pending"  # pending, running, completed, failed
    error: Optional[str] = None


@dataclass
class EngineResult:
    """Result of all batches for a single engine."""
    engine: str
    batches: List[BatchResult] = field(default_factory=list)
    status: str = "pending"  # pending, running, completed, failed
    error: Optional[str] = None
    extra: Dict[str, any] = field(default_factory=dict)

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
    total_duration_s: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "total_duration_s": self.total_duration_s,
            "error": self.error,
            "engines": {name: er.to_dict() for name, er in self.engines.items()},
        }

    def summary_table(self) -> str:
        """Format the same summary table as the original benchmark.sh."""

        def fmt(secs: float) -> str:
            s = int(secs)
            return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"

        lines = [
            "",
            "=== All benchmarks completed successfully ===",
            "",
            f"{'':12s}{'1':12s}{'2':12s}{'3':s}",
        ]
        total = 0.0
        for name, er in self.engines.items():
            b = er.batches
            label = f"{name.capitalize()}:"
            lines.append(
                f"{label:12s}{fmt(b[0].duration_s):s} -> "
                f"{fmt(b[1].duration_s):s} -> "
                f"{fmt(b[2].duration_s):s}"
            )
            total += er.total_duration_s
        lines.append("")
        lines.append(f"================= {fmt(total)} ==================")
        return "\n".join(lines)
