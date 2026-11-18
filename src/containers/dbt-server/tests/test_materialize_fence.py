import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask
from handlers.materialize_emulator import bp
from services.materialize_fence import expected_models, fence_outputs


class MaterializeReplicaTests(unittest.TestCase):
    def test_replica_status_handles_unsigned_catalog_values(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            ("quickstart", "50cc", "1", "1", "1000000000", "8139046912")
        ]
        app = Flask(__name__)
        app.register_blueprint(bp)
        env = {
            "MATERIALIZE_EMULATOR_CLUSTER_SIZE": "50cc",
            "MATERIALIZE_EMULATOR_CPUS": "1",
            "MATERIALIZE_EMULATOR_MEM": "8g",
        }
        with patch.dict(os.environ, env), patch(
            "handlers.materialize_emulator._connection", return_value=connection
        ):
            response = app.test_client().post("/status/materialize-emulator")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["workers"], 1)


class MaterializeManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.paths = [f"gold/model_{i}" for i in range(49)] + ["work/wrk_company_financials"]
        self.targets = self.root / "targets.json"
        self.manifest = self.root / "manifest.json"
        self.targets.write_text(json.dumps({
            "models": [{"engines": ["spark", "materialize-emulator"], "paths": self.paths}]
        }))
        self.nodes = {
            path: {
                "resource_type": "model", "package_name": "tpcdi",
                "original_file_path": f"models/{path}.sql",
                "schema": path.split("/")[0], "alias": path.split("/")[-1],
                "config": {"materialized": "ephemeral" if path.startswith("work/") else "materialized_view"},
            }
            for path in self.paths
        }

    def read_models(self):
        self.manifest.write_text(json.dumps({"nodes": self.nodes}))
        return expected_models(self.manifest, self.targets)

    def test_requires_full_manifest(self):
        self.assertEqual(len(self.read_models()), 49)
        self.nodes.pop("gold/model_1")
        with self.assertRaisesRegex(RuntimeError, "manifest mismatch"):
            self.read_models()

    def test_rejects_accidental_ephemeral_output(self):
        self.nodes["gold/model_1"]["config"]["materialized"] = "ephemeral"
        with self.assertRaisesRegex(RuntimeError, "not a materialized view"):
            self.read_models()

    def test_rejects_extra_model(self):
        self.nodes["extra"] = {
            **self.nodes["gold/model_1"],
            "original_file_path": "models/gold/extra.sql",
            "alias": "extra",
        }
        with self.assertRaisesRegex(RuntimeError, "manifest mismatch"):
            self.read_models()

    def test_rejects_catalog_substitution_even_with_50_targets(self):
        self.targets.write_text(json.dumps({"models": [
            {"engines": ["spark"], "paths": self.paths},
            {"engines": ["materialize-emulator"], "paths": self.paths[1:] + ["gold/extra"]},
        ]}))
        with self.assertRaisesRegex(RuntimeError, "50 standard model targets"):
            self.read_models()

    def test_fence_rejects_missing_outputs(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("strict serializable",)
        cursor.fetchall.return_value = [("gold", "model_1")]
        nodes = [
            {"schema": "gold", "alias": "model_1"},
            {"schema": "gold", "alias": "model_2"},
        ]
        with patch("services.materialize_fence.expected_models", return_value=nodes):
            with self.assertRaisesRegex(RuntimeError, "model_2"):
                fence_outputs(connection)


if __name__ == "__main__":
    unittest.main()
