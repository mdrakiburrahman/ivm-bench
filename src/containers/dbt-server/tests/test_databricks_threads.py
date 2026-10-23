import unittest
from pathlib import Path

import yaml
from jinja2 import Environment


class DatabricksThreadsTest(unittest.TestCase):
    def test_thread_override_and_legacy_default_are_independent_of_spark(self):
        profile = Path(__file__).resolve().parents[1] / "dbt-projects/databricks-enzyme/profiles.yml"
        template = Environment().from_string(profile.read_text())
        for override, expected in ((None, 8), ("1", 1), ("4", 4)):
            for spark_threads in ("", "1", "4", "30"):
                with self.subTest(override=override, spark_threads=spark_threads):
                    def env_var(name, default="test"):
                        if name == "SPARK_DBT_THREADS":
                            return spark_threads
                        return (override or "") if name == "DATABRICKS_DBT_THREADS" else default
                    result = yaml.safe_load(template.render(env_var=env_var))
                    self.assertEqual(int(result["tpcdi"]["outputs"]["databricks-enzyme"]["threads"]), expected)


class DatabricksLayoutTest(unittest.TestCase):
    def test_repeated_layout_and_explicit_refresh_policies(self):
        from jinja2.nativetypes import NativeEnvironment
        project = Path(__file__).resolve().parents[1] / "dbt-projects/databricks-enzyme/dbt_project.yml"
        models = yaml.safe_load(project.read_text())["models"]["tpcdi"]
        layout = NativeEnvironment().from_string(models["gold"]["fact_market_history"]["+partition_by"])
        policy_template = Environment().from_string(models["+refresh_policy"])
        for workload in ("standard", "databricks"):
            for repeated in ("0", "1"):
                for policy in ("AUTO", "FULL", "INCREMENTAL", "INCREMENTAL STRICT"):
                    with self.subTest(workload=workload, repeated=repeated, policy=policy):
                        env = {"TPCDI_WORKLOAD": workload, "REPEATED_REFRESH": repeated, "DATABRICKS_REFRESH_POLICY": policy}
                        context = {"env_var": lambda key, default="": env.get(key, default)}
                        self.assertEqual(layout.render(**context), ["sk_date_id"] if repeated == "1" else [])
                        self.assertEqual(policy_template.render(**context), policy)
                        self.assertNotIn("daily_market", models["silver"])
