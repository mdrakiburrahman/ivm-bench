import json
import os
import sys
import tempfile
import threading
import types
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import databricks_openivm as backend


def model(name, dependencies=()):
    return {"name": name, "database": "cat", "schema": "gold", "resource_type": "model",
            "config": {}, "depends_on": {"nodes": list(dependencies)},
            "compiled_code": "SELECT id FROM cat.data.events"}


class DatabricksConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.enterContext(patch.object(backend, "WORK_DIR", self.directory))
        self.enterContext(patch.object(backend, "_record_node"))
        self.executors = []
        self.lock = threading.Lock()

    def executor(self):
        executor = Mock(connection_s=0.0, statements=[], model_timings=[])
        with self.lock:
            self.executors.append(executor)
        return executor

    def test_ready_models_overlap_and_child_starts_before_unrelated_root_finishes(self):
        roots_started = threading.Barrier(2, timeout=5)
        child_started = threading.Event()
        release_b = threading.Event()
        manifest = {"nodes": {
            "a": model("a"), "b": model("b"), "c": model("c", ["a"]),
        }}
        checkpoint = {"run_id": "test", "dirty": True}
        seen = {}
        version = [4]

        def build(executor, metadata, uid, node, known, state, by_relation, run_id, full_refresh, timings):
            seen[uid] = executor
            executor.version.side_effect = lambda _: version[0]
            if uid in ("a", "b"):
                roots_started.wait()
            if uid == "a":
                self.assertEqual(metadata.read(executor, "version", "cat.gold._openivm_data_a"), 4)
                version[0] = 5
            elif uid == "b":
                self.assertTrue(release_b.wait(5))
            else:
                self.assertEqual(state["a"], {"completed": "a"})
                self.assertNotIn("b", state)
                self.assertEqual(metadata.read(executor, "version", "cat.gold._openivm_data_a"), 5)
                child_started.set()
                release_b.set()
            return {"completed": uid}

        coordinator = threading.get_ident()
        save_threads = []
        save = backend._save

        def checkpoint_write(path, value):
            if path.name == "checkpoint.json":
                save_threads.append(threading.get_ident())
            return save(path, value)

        try:
            with patch.object(backend, "_build_model", side_effect=build), \
                    patch.object(backend, "_save", side_effect=checkpoint_write):
                backend.build_models(manifest, checkpoint, full_refresh=True, batch_num=1,
                                     threads=2, executor_factory=self.executor)
        finally:
            release_b.set()
        self.assertTrue(child_started.is_set())
        self.assertEqual(len(self.executors), 2)
        self.assertIsNot(seen["a"], seen["b"])
        self.assertIs(seen["a"], seen["c"])
        self.assertEqual(save_threads, [coordinator] * 3)
        self.assertEqual(set(checkpoint["models"]), {"a", "b", "c"})
        for executor in self.executors:
            executor.close.assert_called_once()

    def test_failure_drains_active_writes_and_does_not_start_queued_models_or_children(self):
        started = threading.Barrier(2, timeout=5)
        failed = threading.Event()
        release_a = threading.Event()
        manifest = {"nodes": {
            "a": model("a"), "b": model("b"), "c": model("c", ["b"]), "z": model("z"),
        }}
        checkpoint = {"run_id": "test", "dirty": True, "models": {"b": {"source_versions": {"events": 4}}}}
        seen = set()

        def build(executor, metadata, uid, *args):
            with self.lock:
                seen.add(uid)
            self.assertIn(uid, ("a", "b"))
            started.wait()
            if uid == "b":
                failed.set()
                raise RuntimeError("failed maintenance")
            self.assertTrue(release_a.wait(5))
            executor.close.assert_not_called()
            return {"completed": uid}

        with ThreadPoolExecutor(max_workers=1) as controller, \
                patch.object(backend, "_build_model", side_effect=build):
            future = controller.submit(backend.build_models, manifest, checkpoint, full_refresh=False,
                                       batch_num=2, threads=2, executor_factory=self.executor)
            try:
                self.assertTrue(failed.wait(5))
                self.assertFalse(future.done())
            finally:
                release_a.set()
            with self.assertRaisesRegex(RuntimeError, "failed maintenance"):
                future.result(timeout=5)
        self.assertEqual(seen, {"a", "b"})
        self.assertTrue(checkpoint["dirty"])
        self.assertEqual(checkpoint["models"]["b"]["source_versions"], {"events": 4})
        timing = json.loads((self.directory / "sql-batch2.json").read_text())["timing"]
        self.assertEqual({m["model_id"]: m["status"] for m in timing["models"]},
                         {"a": "completed", "b": "failed"})
        for executor in self.executors:
            executor.close.assert_called_once()

    def test_default_and_override_match_databricks_model_concurrency(self):
        manifest = {"nodes": {"a": model("a")}}
        for value, expected in (("", 8), ("3", 3)):
            with self.subTest(value=value), patch.dict(os.environ, {"DATABRICKS_DBT_THREADS": value}), \
                    patch.object(backend, "ThreadPoolExecutor", wraps=ThreadPoolExecutor) as pool, \
                    patch.object(backend, "_build_model", return_value={}):
                backend.build_models(manifest, {"run_id": "test"}, full_refresh=True, batch_num=1,
                                     executor_factory=self.executor)
            pool.assert_called_once_with(max_workers=expected)

    def test_shared_source_metadata_is_read_once_during_initialization(self):
        manifest = {"nodes": {"a": model("a"), "b": model("b")}, "sources": {
            "source.events": {"name": "events", "database": "cat", "schema": "data"},
        }}

        def executor():
            result = self.executor()
            result.schema.return_value = [{"name": "id", "ddl_type": "bigint"}]
            result.version.return_value = 4
            return result

        compiled = {"refresh_type": 2, "initial_sql": "SELECT id FROM openivm_compile.events",
                    "compiled_sql": "maintenance", "output_schema": {"fields": [{"name": "id"}]}}
        with patch.object(backend, "compiler", return_value=compiled):
            backend.build_models(manifest, {"run_id": "test"}, full_refresh=True, batch_num=1,
                                 threads=2, executor_factory=executor)
        self.assertEqual(sum(e.schema.call_count for e in self.executors), 1)
        self.assertEqual(sum(e.version.call_count for e in self.executors), 1)

    def test_all_sessions_close_and_diagnostics_survive_a_close_error(self):
        barrier = threading.Barrier(2, timeout=5)
        manifest = {"nodes": {"a": model("a"), "b": model("b")}}

        def build(executor, *args):
            barrier.wait()
            return {}

        def executor():
            result = self.executor()
            result.close.side_effect = RuntimeError("close failed")
            return result

        with patch.object(backend, "_build_model", side_effect=build), \
                self.assertRaisesRegex(RuntimeError, "close failed"):
            backend.build_models(manifest, {"run_id": "test"}, full_refresh=True,
                                 batch_num=1, threads=2, executor_factory=executor)
        self.assertEqual(len(self.executors), 2)
        for executor in self.executors:
            executor.close.assert_called_once()
        self.assertEqual(len(json.loads((self.directory / "sql-batch1.json").read_text())["timing"]["models"]), 2)


class BatchMetadataTest(unittest.TestCase):
    def test_concurrent_readers_share_one_read_and_receive_the_same_error(self):
        for fail in (False, True):
            with self.subTest(fail=fail):
                entered = threading.Event()
                waiting = threading.Event()
                release = threading.Event()

                class WatchedFuture(Future):
                    def result(self, timeout=None):
                        if not self.done():
                            waiting.set()
                        return super().result(timeout)

                executor = Mock()

                def read(_):
                    entered.set()
                    self.assertTrue(release.wait(5))
                    if fail:
                        raise ValueError("metadata failed")
                    return 4

                executor.version.side_effect = read
                cache = backend.BatchMetadata()
                with patch.object(backend, "Future", WatchedFuture), ThreadPoolExecutor(max_workers=2) as pool:
                    first = pool.submit(cache.read, executor, "version", "table")
                    try:
                        self.assertTrue(entered.wait(5))
                        second = pool.submit(cache.read, executor, "version", "table")
                        self.assertTrue(waiting.wait(5))
                    finally:
                        release.set()
                    for future in (first, second):
                        if fail:
                            with self.assertRaisesRegex(ValueError, "metadata failed"):
                                future.result(timeout=5)
                        else:
                            self.assertEqual(future.result(timeout=5), 4)
                executor.version.assert_called_once_with("table")

    def test_versions_are_invalidated_after_writes_and_not_reused_across_batches(self):
        executor = Mock()
        executor.version.side_effect = [4, 5, 6]
        cache = backend.BatchMetadata()
        self.assertEqual(cache.read(executor, "version", "table"), 4)
        self.assertEqual(cache.read(executor, "version", "table"), 4)
        cache.invalidate_version("table")
        self.assertEqual(cache.read(executor, "version", "table"), 5)
        self.assertEqual(backend.BatchMetadata().read(executor, "version", "table"), 6)
        self.assertEqual(executor.version.call_count, 3)


class WorkerSessionTest(unittest.TestCase):
    def setUp(self):
        import services

        self.sources = types.ModuleType("services.databricks_enzyme_sources")
        self.sources.open_connection = Mock()
        self.enterContext(patch.object(services, "databricks_enzyme_sources", self.sources, create=True))

    def test_workers_open_independent_sessions_and_cdf_reuse_is_session_local(self):
        self.sources.open_connection.side_effect = [Mock(), Mock()]
        first = backend.SqlExecutor()
        second = backend.SqlExecutor()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        self.assertEqual(self.sources.open_connection.call_count, 2)
        self.assertIsNot(first.connection, second.connection)
        first.execute = Mock()
        second.execute = Mock()
        fields = [{"name": "id"}]
        sql = backend._cdf_sql("events", "cat.data.events", fields, 4, 5)
        first.ensure_delta_view("events", sql)
        first.ensure_delta_view("events", sql)
        first.execute.assert_called_once_with(sql, phase="cdf_setup")
        second.ensure_delta_view("events", sql)
        second.execute.assert_called_once_with(sql, phase="cdf_setup")
        for changed in (
            backend._cdf_sql("events", "cat.data.events", fields, 5, 6),
            backend._cdf_sql("events", "cat.data.events", fields, 5, 6, "id > 0"),
            backend._cdf_sql("events", "cat.other.events", fields, 5, 6),
        ):
            first.ensure_delta_view("events", changed)
        self.assertEqual(first.execute.call_count, 4)

    def test_metadata_queries_keep_query_ids_phase_and_model_timing(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.query_id = "metadata-query"
        cursor.description = [("version",)]
        cursor.fetchall.return_value = [(7,)]
        self.sources.open_connection.return_value = connection
        executor = backend.SqlExecutor()
        self.addCleanup(executor.close)
        executor.model_id = "model.a"
        self.assertEqual(executor.version("table"), 7)
        statement, = executor.statements
        self.assertEqual(statement["query_id"], "metadata-query")
        self.assertEqual(statement["phase"], "metadata")
        self.assertEqual(statement["model_id"], "model.a")
        self.assertGreaterEqual(statement["duration_s"], 0)


if __name__ == "__main__":
    unittest.main()
