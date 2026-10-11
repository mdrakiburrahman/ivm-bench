import unittest
from pathlib import Path

import yaml
from jinja2 import Environment


class DatabricksThreadsTest(unittest.TestCase):
    def test_thread_override_and_legacy_default(self):
        profile = Path(__file__).resolve().parents[1] / "dbt-projects/databricks-enzyme/profiles.yml"
        template = Environment().from_string(profile.read_text())
        for override, expected in ((None, 8), ("1", 1), ("4", 4)):
            with self.subTest(override=override):
                def env_var(name, default="test"):
                    return (override or "") if name == "SPARK_DBT_THREADS" else default
                result = yaml.safe_load(template.render(env_var=env_var))
                self.assertEqual(int(result["tpcdi"]["outputs"]["databricks-enzyme"]["threads"]), expected)
