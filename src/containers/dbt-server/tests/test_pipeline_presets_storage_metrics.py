import json
import sys
import unittest
from pathlib import Path


DBT_SERVER = Path(__file__).resolve().parents[1]
EXPERIMENTS = DBT_SERVER.parent / "benchmark-server" / "experiments"
sys.path.insert(0, str(DBT_SERVER))

from services import storage_metrics  # noqa: E402


PIPELINE_PRESETS = ("gci-sf10-local-engines.json", "ci-sf10-all-engines.json")


class PipelinePresetsStorageMetricsTest(unittest.TestCase):
    """The runner fails an engine whose storage metrics are not `ok`, so a pipeline
    preset must never select an engine the collector cannot measure. DbspNet once
    did, and GCI only found out after two hours."""

    def test_every_preset_engine_is_supported_by_the_storage_collector(self):
        for name in PIPELINE_PRESETS:
            with self.subTest(preset=name):
                preset = json.loads((EXPERIMENTS / name).read_text())
                baseline = preset["baseline"]
                for experiment in preset["experiments"]:
                    flags = {**baseline.get("feature_flags", {}), **experiment.get("feature_flags", {})}
                    if not flags.get("storage_metrics", True):
                        continue
                    engines = set(experiment.get("engines", baseline["engines"]))
                    self.assertEqual(
                        sorted(engines - storage_metrics.SUPPORTED_ENGINES),
                        [],
                        f"{name}: storage_metrics is on but the collector does not support these engines",
                    )


if __name__ == "__main__":
    unittest.main()
