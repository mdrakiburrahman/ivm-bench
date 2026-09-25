import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models.result import EngineResult
from services.engine_runner import EngineRunner


class FabricProfileExportTest(unittest.TestCase):
    def runner(self, root, engine="fabric-openivm-jvm-35"):
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name=engine)
        runner._config = SimpleNamespace(repo_dir=root, scale_factor=100)
        runner._dbt_url = "http://dbt"
        runner._emit = Mock()
        runner._result = EngineResult(engine=engine)
        return runner

    @patch("services.engine_runner.requests.post")
    def test_profiles_and_sql_use_engine_specific_artifacts(self, post):
        for engine in ("fabric-openivm-jvm-35", "spark-openivm"):
            with self.subTest(engine=engine), tempfile.TemporaryDirectory() as root:
                runner = self.runner(root, engine)
                post.return_value = Mock(status_code=200)
                post.return_value.json.return_value = {
                    "status": "ok", "row_count": 1, "csv": {"profile": "profile-data"}
                }
                runner._export_spark_openivm_profile("run", 2)
                self.assertEqual(post.call_args.args[0], f"http://dbt/profile/{engine}/run/2")
                base = Path(root) / "mount/results/100"
                self.assertEqual((base / f"dbt-server/{engine}-profile-batch2.csv").read_text(), "profile-data")
                post.return_value.json.return_value = {"status": "ok", "rows": [{
                    "view_name": "v", "refresh_id": "refresh_1", "stmt_order": 1,
                    "attempt_idx": 0, "category": "apply", "sql_text": "SELECT 1",
                }]}
                runner._export_spark_openivm_query_log("run", 2)
                self.assertEqual(post.call_args.args[0], f"http://dbt/query-log/{engine}/run/2")
                sql = list((base / engine / "query-log").rglob("*.sql"))
                self.assertEqual(len(sql), 1)
                self.assertIn("SELECT", sql[0].read_text())

    @patch.dict(os.environ, {"OPENIVM_PROFILE_REFRESH": "1", "OPENIVM_QUERY_LOG": "1"})
    def test_fabric_exports_are_after_batch_timer(self):
        runner = self.runner("unused")
        for method in ("_persist_batch_result", "_batch_loader_append", "_capture_delta_stats",
                       "_save_openivm_ops_chart", "_capture_storage_metrics",
                       "_ensure_cpu_measurement_status"):
            setattr(runner, method, Mock())
        now = [100.0]
        def run_batch(_batch):
            now[0] += 10.0
            return "run"
        def export(*_args):
            self.assertEqual(runner._result.batches[1].duration_s, 10.0)
            now[0] += 50.0
        runner._run_fabric = Mock(side_effect=run_batch)
        runner._export_spark_openivm_profile = Mock(side_effect=export)
        runner._export_spark_openivm_query_log = Mock(side_effect=export)
        with patch("services.engine_runner.time.time", side_effect=lambda: now[0]):
            runner._run_batch(2)
        runner._export_spark_openivm_profile.assert_called_once_with("run", 2)
        runner._export_spark_openivm_query_log.assert_called_once_with("run", 2)
        self.assertEqual(runner._result.batches[1].duration_s, 10.0)
        self.assertEqual(runner._result.batches[1].status, "completed")

    @patch("services.engine_runner.requests.post")
    def test_failed_export_is_not_saved_as_success(self, post):
        with tempfile.TemporaryDirectory() as root:
            runner = self.runner(root)
            post.return_value = Mock(status_code=500)
            post.return_value.json.return_value = {"status": "error", "error": "session gone"}
            with self.assertRaisesRegex(RuntimeError, "session gone"):
                runner._export_spark_openivm_profile("run", 2)
            self.assertFalse((Path(root) / "mount").exists())


if __name__ == "__main__":
    unittest.main()
