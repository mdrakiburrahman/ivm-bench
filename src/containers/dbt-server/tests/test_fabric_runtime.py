import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import fabric


class FabricRuntimePinTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch("services.fabric._env_base", return_value="https://example.invalid/environment"))
        self.enterContext(patch("services.fabric._fabric_headers", return_value={}))
        self.request = self.enterContext(patch("services.fabric._fabric_req"))
        self.properties = {"spark.openivm.enabled": "true"}
        self.initial = Mock(status_code=200)
        self.initial.json.return_value = {"runtimeVersion": "2.0"}
        self.updated = Mock(status_code=200)
        self.updated.json.return_value = {"runtimeVersion": "1.3", "sparkProperties": [
            {"key": "spark.openivm.enabled", "value": "true"}
        ]}
        self.request.side_effect = [self.initial, Mock(status_code=200), self.updated]

    def test_pin_uses_stable_contract_and_verifies_properties(self):
        fabric._stage_spark_compute(self.properties)
        args, kwargs = self.request.call_args_list[1]
        self.assertEqual(args[0], "PATCH")
        self.assertEqual(kwargs["params"], {"beta": "false"})
        self.assertEqual(kwargs["json"], {"runtimeVersion": "1.3", "sparkProperties": [
            {"key": "spark.openivm.enabled", "value": "true"}
        ]})
        self.assertEqual(self.request.call_count, 3)

    def test_runtime_drift_is_rejected(self):
        self.updated.json.return_value["runtimeVersion"] = "2.0"
        with self.assertRaisesRegex(RuntimeError, "runtime pin not applied"):
            fabric._stage_spark_compute(self.properties)

    def test_missing_or_wrong_property_is_rejected(self):
        self.updated.json.return_value["sparkProperties"] = []
        with self.assertRaisesRegex(RuntimeError, "Spark properties not applied"):
            fabric._stage_spark_compute(self.properties)

    def test_baseline_also_pins_runtime_and_requires_publish_success(self):
        stage = self.enterContext(patch("services.fabric._stage_spark_compute"))
        self.request.side_effect = None
        self.request.return_value = Mock(status_code=400)
        with self.assertRaisesRegex(RuntimeError, "baseline environment publish failed"):
            fabric.publish_empty_environment()
        stage.assert_called_once_with({})

    def test_async_compute_update_is_awaited_before_readback(self):
        poll = self.enterContext(patch("services.fabric._lro_poll"))
        self.request.side_effect = [self.initial, Mock(status_code=202, headers={"Location": "https://example.invalid/operation"}), self.updated]
        fabric._stage_spark_compute(self.properties)
        poll.assert_called_once_with("https://example.invalid/operation")


