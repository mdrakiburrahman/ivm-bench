import json
import re
import sqlite3
import sys
import unittest
from pathlib import Path

BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
DBT_PROJECTS = BENCHMARK_SERVER.parent / "dbt-server" / "dbt-projects"
REPO = BENCHMARK_SERVER.parents[2]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.experiments import parse_experiments_json  # noqa: E402


def action_type_expressions(engine):
    model = (
        DBT_PROJECTS / engine / "models" / "bronze" / "crm"
        / "crm_customer_mgmt.sql"
    ).read_text(encoding="utf-8")
    expressions = re.findall(
        r"(case\s+(?:when .*\s+)+?end) as action_type", model,
    )
    return [" ".join(expression.split()) for expression in expressions]


def render_model(engine, relative_path, batch_2_days):
    model = (DBT_PROJECTS / engine / relative_path).read_text(encoding="utf-8")
    conditional = re.compile(
        r"{%\s*if\s+env_var\('TPCDI_BATCH_2_DAYS',\s*'0'\)\s*\|\s*int\s*>\s*0\s*%}"
        r"(.*?)(?:{%\s*else\s*%}(.*?))?{%\s*endif\s*%}",
        re.DOTALL,
    )
    model = conditional.sub(
        lambda match: match.group(1) if batch_2_days > 0 else (match.group(2) or ""),
        model,
    )
    model = re.sub(
        r"{{\s*source\('tpcdi',\s*'([^']+)'\)\s*}}", r"\1", model,
    )
    return re.sub(r"{{\s*ref\('([^']+)'\)\s*}}", r"\1", model)


def normalized_sql(sql):
    without_comments = re.sub(r"--[^\n]*", "", sql)
    return " ".join(without_comments.split())


class AugmentedTpcdiTest(unittest.TestCase):
    def test_databricks_refresh_policy_works_for_standard_tpcdi(self):
        experiment = parse_experiments_json(json.dumps({
            "experiments": [{
                "batch_2_days": 0,
                "databricks_refresh_policy": "full",
            }],
        }))[0]

        self.assertEqual(experiment.databricks_refresh_policy, "FULL")
        self.assertEqual(
            experiment.to_compose_env()["DATABRICKS_REFRESH_POLICY"], "FULL",
        )
        self.assertFalse(hasattr(experiment.compiler_bench, "databricks_refresh_policy"))

    def test_invalid_databricks_refresh_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "databricks_refresh_policy"):
            parse_experiments_json(json.dumps({
                "experiments": [{"databricks_refresh_policy": "sometimes"}],
            }))

    def test_databricks_refresh_policy_reaches_dbt(self):
        compose = (
            REPO / "docker" / "docker-compose.benchmark.databricks-enzyme.yml"
        ).read_text(encoding="utf-8")
        project = (
            DBT_PROJECTS / "databricks-enzyme" / "dbt_project.yml"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'DATABRICKS_REFRESH_POLICY: "${DATABRICKS_REFRESH_POLICY:-AUTO}"',
            compose,
        )
        self.assertIn("env_var('DATABRICKS_REFRESH_POLICY', 'AUTO')", project)

    def test_databricks_policy_sweeps_are_isolated_and_comparable(self):
        experiment_dir = BENCHMARK_SERVER / "experiments"
        policies = {}
        for policy in ("full", "incremental"):
            config = experiment_dir / f"sf100-augmented-databricks-{policy}.json"
            experiments = parse_experiments_json(config.read_text(encoding="utf-8"))
            policies[policy] = [experiment.databricks_refresh_policy for experiment in experiments]
            self.assertEqual(
                [(experiment.scale_factor, experiment.batch_2_days) for experiment in experiments],
                [(100, 18), (100, 55), (100, 91), (100, 128), (100, 164)],
            )
            for experiment in experiments:
                self.assertEqual(experiment.engines, ["databricks-enzyme"])
                self.assertFalse(experiment.feature_flags.openivm_validate)
                self.assertTrue(experiment.feature_flags.openivm_profile_refresh)
                self.assertEqual(experiment.batch_2_update_pct, "0")
                self.assertEqual(experiment.batch_2_delete_pct, "0")

        self.assertEqual(policies["full"], ["FULL"] * 5)
        self.assertEqual(policies["incremental"], ["INCREMENTAL"] * 5)

    def test_databricks_policy_is_not_a_duplicate_workflow_input(self):
        workflow = (REPO / ".github" / "workflows" / "gci.yaml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("inputs.databricks_refresh_policy", workflow)
        self.assertIn("inputs.experiments_file || 'inline'", workflow)
        dispatch_inputs = re.search(
            r"(?ms)^  workflow_dispatch:\n    inputs:\n(.*?)(?=^\S)", workflow,
        ).group(1)
        self.assertLessEqual(
            len(re.findall(r"(?m)^      [a-zA-Z0-9_]+:", dispatch_inputs)),
            25,
        )

    def test_days_are_forwarded_to_datagen(self):
        experiments = parse_experiments_json(json.dumps({
            "experiments": [{"scale_factor": 3, "batch_2_days": 37}],
        }))
        self.assertEqual(experiments[0].to_compose_env()["TPCDI_BATCH_2_DAYS"], "37")
        self.assertEqual(experiments[0].to_dict()["batch_2_days"], 37)

    def test_negative_daily_batch_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "between 0 and 364"):
            parse_experiments_json(json.dumps({
                "experiments": [{"batch_2_days": -1}],
            }))

    def test_daily_window_above_databricks_horizon_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "between 0 and 364"):
            parse_experiments_json(json.dumps({
                "experiments": [{"batch_2_days": 365}],
            }))

    def test_sf3_sweep_uses_nearest_whole_days(self):
        config = BENCHMARK_SERVER / "experiments" / "sf3-augmented-percent-sweep.json"
        experiments = parse_experiments_json(config.read_text())
        self.assertEqual(
            [experiment.batch_2_days for experiment in experiments],
            [37, 73, 110, 146, 183],
        )

    def test_sf10_gci_sweep_uses_5_and_50_percent_windows(self):
        config = BENCHMARK_SERVER / "experiments" / "sf10-augmented-5-50.json"
        experiments = parse_experiments_json(config.read_text())
        self.assertEqual(
            [(experiment.scale_factor, experiment.batch_2_days) for experiment in experiments],
            [(10, 18), (10, 183)],
        )

    def test_sf100_sweep_uses_requested_insert_windows(self):
        config = BENCHMARK_SERVER / "experiments" / "sf100-augmented-daily-sweep.json"
        experiments = parse_experiments_json(config.read_text())
        self.assertEqual(
            [(experiment.scale_factor, experiment.batch_2_days) for experiment in experiments],
            [(100, 18), (100, 55), (100, 91), (100, 128), (100, 164)],
        )
        for experiment in experiments:
            self.assertFalse(experiment.feature_flags.openivm_validate)
            self.assertTrue(experiment.feature_flags.openivm_profile_refresh)
            self.assertEqual(experiment.batch_2_update_pct, "0")
            self.assertEqual(experiment.batch_2_delete_pct, "0")

    def test_inactive_customer_and_account_events_are_preserved(self):
        customer, account = action_type_expressions("duckdb")
        connection = sqlite3.connect(":memory:")
        connection.execute("create table customer_events(cdc_flag, status)")
        connection.execute("create table account_events(cdc_flag, ca_st_id)")
        events = (("I", "ACTV"), ("U", "ACTV"), ("U", "INAC"))
        connection.executemany("insert into customer_events values (?, ?)", events)
        connection.executemany("insert into account_events values (?, ?)", events)
        customer_actions = connection.execute(
            f"select {customer} from customer_events"
        ).fetchall()
        account_actions = connection.execute(
            f"select {account} from account_events"
        ).fetchall()
        connection.close()

        self.assertEqual(customer_actions, [("NEW",), ("UPDCUST",), ("INACT",)])
        self.assertEqual(account_actions, [("ADDACCT",), ("UPDACCT",), ("CLOSEACCT",)])

    def test_all_engines_use_the_same_action_type_mapping(self):
        expected = action_type_expressions("duckdb")
        engines = (
            "duckdb-openivm", "spark", "spark-openivm", "feldera",
            "databricks-enzyme", "fabric-jvm-35", "fabric-openivm-jvm-35",
        )
        for engine in engines:
            with self.subTest(engine=engine):
                self.assertEqual(action_type_expressions(engine), expected)

    def test_dbt_server_container_receives_augmented_window(self):
        compose = (REPO / "docker" / "docker-compose.base.yml").read_text()
        self.assertIn(
            'TPCDI_BATCH_2_DAYS: "${TPCDI_BATCH_2_DAYS:-0}"', compose,
        )

    def test_all_engines_use_the_same_augmented_trade_adapter(self):
        models = (
            "models/bronze/brokerage/brokerage_trade.sql",
            "models/bronze/brokerage/brokerage_trade_history.sql",
        )
        engines = (
            "duckdb-openivm", "spark", "spark-openivm", "feldera",
            "databricks-enzyme", "fabric-jvm-35", "fabric-openivm-jvm-35",
        )
        for model in models:
            expected = normalized_sql(render_model("duckdb", model, 3))
            for engine in engines:
                with self.subTest(model=model, engine=engine):
                    actual = normalized_sql(render_model(engine, model, 3))
                    self.assertEqual(actual, expected)

    def test_fabric_incremental_appends_match_columns_by_name(self):
        engines = ("fabric-jvm-35", "fabric-openivm-jvm-35")
        for engine in engines:
            macro = (
                DBT_PROJECTS / engine / "macros" / "load_fabric_sources.sql"
            ).read_text(encoding="utf-8")
            with self.subTest(engine=engine):
                self.assertIn(
                    "INSERT INTO {{ db }}.staging_{{ t }} BY NAME SELECT *",
                    macro,
                )

    def test_augmented_trade_events_reach_trade_history_without_multiplication(self):
        connection = sqlite3.connect(":memory:")
        connection.execute("""
            create table staging_trade(
                cdc_flag varchar, cdc_dsn bigint, t_id bigint, t_dts timestamp,
                t_st_id varchar, t_tt_id varchar, t_is_cash boolean,
                t_s_symb varchar, t_qty integer, t_bid_price double,
                t_ca_id bigint, t_exec_name varchar, t_trade_price double,
                t_chrg double, t_comm double, t_tax double
            )
        """)
        rows = (
            (None, None, 1, "2016-07-01 09:00:00", "SBMT"),
            ("U", 1, 1, "2016-07-07 10:05:00", "CMPT"),
            ("I", 2, 2, "2016-07-07 10:00:00", "SBMT"),
            ("U", 3, 2, "2016-07-07 10:05:00", "CMPT"),
        )
        connection.executemany("""
            insert into staging_trade values (
                ?, ?, ?, ?, ?, 'TMB', true, 'SYM', 10, 1.0, 100,
                'executor', 2.0, 0.1, 0.2, 0.3
            )
        """, rows)
        connection.execute("""
            create table batch1_trade_history(
                th_t_id bigint, th_dts timestamp, th_st_id varchar
            )
        """)
        connection.executemany(
            "insert into batch1_trade_history values (?, ?, ?)",
            ((1, "2016-07-01 08:00:00", "PNDG"),
             (1, "2016-07-01 09:00:00", "SBMT")),
        )

        trade_model = "models/bronze/brokerage/brokerage_trade.sql"
        history_model = "models/bronze/brokerage/brokerage_trade_history.sql"
        connection.execute(
            "create table standard_brokerage_trade as "
            + render_model("duckdb", trade_model, 0)
        )
        connection.execute(
            "create table standard_brokerage_trade_history as "
            + render_model("duckdb", history_model, 0)
        )
        connection.execute(
            "create table brokerage_trade as "
            + render_model("duckdb", trade_model, 3)
        )
        connection.execute(
            "create table brokerage_trade_history as "
            + render_model("duckdb", history_model, 3)
        )
        self.assertEqual(
            connection.execute("select count(*) from standard_brokerage_trade").fetchone(),
            (4,),
        )
        self.assertEqual(
            connection.execute(
                "select count(*) from standard_brokerage_trade_history"
            ).fetchone(),
            (2,),
        )
        self.assertEqual(
            connection.execute(
                "select t_id, t_st_id from brokerage_trade order by t_id"
            ).fetchall(),
            [(1, "CMPT"), (2, "CMPT")],
        )
        self.assertEqual(
            connection.execute("""
                select th_t_id, th_st_id
                from brokerage_trade_history
                order by th_t_id, th_dts
            """).fetchall(),
            [(1, "PNDG"), (1, "SBMT"), (1, "CMPT"), (2, "SBMT"), (2, "CMPT")],
        )
        self.assertEqual(
            connection.execute(
                """
                select t.t_id, count(*)
                from brokerage_trade t
                join brokerage_trade_history h on t.t_id = h.th_t_id
                group by t.t_id
                order by t.t_id
                """
            ).fetchall(),
            [(1, 3), (2, 2)],
        )
        connection.close()


if __name__ == "__main__":
    unittest.main()
