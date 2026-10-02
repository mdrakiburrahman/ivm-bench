import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from threading import Event
from types import SimpleNamespace


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.engine_runner import (  # noqa: E402
    _post_fabric_cache_with_retry, EngineRunner,
)


class FabricCacheCoordinationTest(unittest.TestCase):
    @patch("services.engine_runner._post_with_retry")
    def test_shared_cache_post_waits_for_lock(self, post):
        inside_lock = False

        class Guard:
            def __enter__(self):
                nonlocal inside_lock
                inside_lock = True

            def __exit__(self, *_args):
                nonlocal inside_lock
                inside_lock = False

        def assert_locked(*_args):
            self.assertTrue(inside_lock)

        post.side_effect = assert_locked
        with patch("services.engine_runner._FABRIC_CACHE_LOCK", Guard()):
            _post_fabric_cache_with_retry("url", lambda _message: None, "fabric")

        post.assert_called_once()

    def test_both_fabric_engines_overlap_warmup_with_cache_and_wait_before_build(self):
        for engine in ("fabric-openivm-jvm-35", "fabric-jvm-35"):
            with self.subTest(engine=engine):
                runner = EngineRunner.__new__(EngineRunner)
                runner._engine = SimpleNamespace(name=engine)
                runner._config = SimpleNamespace(scale_factor=100)
                runner._dbt_url = "http://dbt"
                runner._emit = Mock()
                runner._read_last_sf = Mock(return_value=None)
                runner._write_last_sf = Mock()
                entered, seeded = Event(), Event()
                def post(url, **kwargs):
                    if url.endswith("warm-session"):
                        self.assertEqual(kwargs["json"], {"engine": engine})
                        entered.set()
                        self.assertTrue(seeded.wait(5))
                        return Mock(json=Mock(return_value={"duration_s": 145}))
                    return Mock(status_code=200, json=Mock(return_value={"resolved": {}}))
                def seed(*args):
                    self.assertTrue(entered.wait(5))
                    seeded.set()
                    return Mock(json=Mock(return_value={"already_seeded": True}))
                def build(*args):
                    self.assertTrue(seeded.is_set())
                    return "run"
                runner._run_fabric_dbt = Mock(side_effect=build)
                with patch("services.engine_runner.requests.post", side_effect=post), \
                     patch("services.engine_runner._post_fabric_cache_with_retry", side_effect=seed):
                    self.assertEqual(runner._run_fabric(1), "run")
                runner._run_fabric_dbt.assert_called_once_with(1, True)
                runner._write_last_sf.assert_called_once_with(100)

    def test_failed_warmup_prevents_build(self):
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name="fabric-jvm-35")
        runner._config = SimpleNamespace(scale_factor=100)
        runner._dbt_url = "http://dbt"
        runner._emit = Mock()
        runner._read_last_sf = Mock(return_value=None)
        runner._run_fabric_dbt = Mock()
        ready = Mock()
        ready.raise_for_status.side_effect = RuntimeError("warmup failed")
        def post(url, **kwargs):
            return ready if url.endswith("warm-session") else Mock(status_code=200, json=Mock(return_value={}))
        with patch("services.engine_runner.requests.post", side_effect=post), \
             patch("services.engine_runner._post_fabric_cache_with_retry"):
            with self.assertRaisesRegex(RuntimeError, "warmup failed"):
                runner._run_fabric(1)
        runner._run_fabric_dbt.assert_not_called()


if __name__ == "__main__":
    unittest.main()
