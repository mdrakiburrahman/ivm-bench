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


class DatabricksRefreshPolicyTest(unittest.TestCase):
    def test_repeated_auto_keeps_window_source_incremental(self):
        project = Path(__file__).resolve().parents[1] / "dbt-projects/databricks-enzyme/dbt_project.yml"
        models = yaml.safe_load(project.read_text())["models"]["tpcdi"]
        window_policy = Environment().from_string(models["silver"]["daily_market"]["+refresh_policy"])
        baseline_policy = Environment().from_string(models["+refresh_policy"])
        for workload in ("standard", "databricks"):
            for repeated in ("0", "1"):
                for policy in ("AUTO", "FULL", "INCREMENTAL", "INCREMENTAL STRICT"):
                    with self.subTest(workload=workload, repeated=repeated, policy=policy):
                        env = {"TPCDI_WORKLOAD": workload, "REPEATED_REFRESH": repeated, "DATABRICKS_REFRESH_POLICY": policy}
                        context = {"env_var": lambda key, default="": env.get(key, default)}
                        expected = "INCREMENTAL STRICT" if repeated == "1" and policy == "AUTO" else policy
                        self.assertEqual(window_policy.render(**context), expected)
                        self.assertEqual(baseline_policy.render(**context), policy)
