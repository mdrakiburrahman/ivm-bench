import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services import storage_metrics  # noqa: E402


def _delta_table(root: Path, name: str, data_bytes: int) -> None:
    table = root / name
    (table / "_delta_log").mkdir(parents=True)
    (table / "_delta_log" / "00000000000000000000.json").write_text('{"commitInfo":{}}\n')
    (table / "part-00000.parquet").write_bytes(b"\0" * data_bytes)


class DbspNetStorageMetricsTest(unittest.TestCase):
    """DbspNet writes its outputs as Delta under /data/processed/dbspnet and reads its
    inputs straight from the shared raw Delta tables, like Feldera. Before it was
    registered, the collector answered status=unsupported and the GCI runner failed
    the engine after a successful batch."""

    def test_dbspnet_is_collected_like_a_raw_backed_local_engine(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed = Path(tmp) / "processed" / "dbspnet"
            raw = Path(tmp) / "raw"
            _delta_table(processed, "dim_account", 1000)
            _delta_table(raw / "batch1", "hr", 500)
            _delta_table(raw / "staging", "trade", 700)

            with patch.dict(storage_metrics.PROCESSED_ROOTS, {"dbspnet": processed}), \
                    patch.object(storage_metrics, "RAW_DELTA_ROOT", raw):
                result = storage_metrics.collect_storage_metrics("dbspnet", batch_num=1)

        self.assertEqual(result["status"], "ok", result.get("errors"))
        self.assertGreater(result["totals"]["visible_output_bytes"], 0)
        self.assertEqual(result["base_tables"]["source_mode"], "shared_raw")
        self.assertGreater(result["base_tables"]["reference_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
