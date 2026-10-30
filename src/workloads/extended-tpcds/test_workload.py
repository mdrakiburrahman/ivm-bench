"""Execute the workload over small TPC-DS-shaped inputs, without downloads."""

import json
import unittest
from decimal import Decimal
from datetime import date

import duckdb

import generate
import pipeline


def fixture():
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA base")
    connection.execute("""CREATE TABLE base.customer AS
        SELECT n+1 AS c_customer_sk, 'customer-' || n AS c_customer_id FROM range(10) c(n)""")
    connection.execute("""CREATE TABLE base.item AS
        SELECT n+1 AS i_item_sk, 'item-' || (n//2) AS i_item_id,
               'Retail' AS i_category, CAST(12.34 AS DECIMAL(7,2)) AS i_current_price,
               'Item ' || (n//2) AS i_item_desc FROM range(6) i(n)""")
    connection.execute("""CREATE TABLE base.date_dim AS
        SELECT n+1 AS d_date_sk, DATE '2002-11-01'+n::INTEGER AS d_date FROM range(61) d(n)""")
    connection.execute("CREATE TABLE base.time_dim AS SELECT 1 AS t_time_sk, 50400 AS t_time")
    for table, prefix, customer, order in [
        ("store_sales", "ss", "customer_sk", "ticket_number"),
        ("catalog_sales", "cs", "bill_customer_sk", "order_number"),
        ("web_sales", "ws", "bill_customer_sk", "order_number"),
    ]:
        connection.execute(f"""CREATE TABLE base.{table} AS
            SELECT CASE WHEN d.n=0 THEN 1 WHEN d.n=1 THEN 15 ELSE 61 END AS {prefix}_sold_date_sk,
                   1 AS {prefix}_sold_time_sk, i.n+1 AS {prefix}_item_sk,
                   c.n+1 AS {prefix}_{customer}, c.n*10+d.n AS {prefix}_{order},
                   2 AS {prefix}_quantity, CAST(12.34 AS DECIMAL(7,2)) AS {prefix}_sales_price
            FROM range(10) c(n) CROSS JOIN range(6) i(n) CROSS JOIN range(3) d(n)""")
    connection.execute("INSERT INTO base.store_sales VALUES (61,1,1,NULL,999,3,12.34),(NULL,1,1,1,998,4,12.34)")
    return connection


class WorkloadTests(unittest.TestCase):
    def test_sql_dependencies_match_mapping(self):
        configuration = {"report_date": "2002-12-31", "history_months": 12}
        with duckdb.connect() as connection:
            for name, row in pipeline.mapping().items():
                actual = connection.get_table_names(pipeline.render_model(name, configuration))
                expected = set(filter(None, row["parent_models"].split(";")))
                if row["extension_source"]:
                    expected.add(row["extension_source"])
                if name == "meta_ingestion":
                    self.assertEqual(actual, set())
                    self.assertEqual(len(expected), 4)
                else:
                    self.assertEqual(actual, expected, name)

    def test_normalization_preserves_lines_nulls_and_decimal_prices(self):
        with fixture() as connection:
            config = generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            stats = generate.statistics(connection)
            self.assertEqual(stats["normalized_purchase_rows"], 542)
            self.assertEqual(stats["missing_customer_rows"], 1)
            self.assertEqual(stats["quarantined_missing_date_rows"], 1)
            self.assertEqual(stats["extension_rows"]["ext_purchase_usage_events"], 541)
            self.assertEqual(connection.execute("SELECT typeof(original_sales_price), min(original_sales_price) FROM normalized_purchase GROUP BY ALL").fetchone(), ("DECIMAL(7,2)", Decimal("12.34")))
            self.assertEqual(connection.execute("SELECT count(*) FROM xt_accounts WHERE customer_id='unknown'").fetchone()[0], 2)
            self.assertFalse(config["base_rows_transformed"])

    def test_all_models_and_independent_cardinality_checks(self):
        with fixture() as connection:
            generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            result = pipeline.build(connection)
            self.assertEqual(len(result["model_rows"]), 96)
            checks = pipeline.validate(connection, result["model_rows"])
            self.assertEqual(len(checks), 10)
            self.assertGreater(checks["monthly_quantity"]["actual_rows"], 0)
            self.assertEqual(result["model_rows"]["meta_ingestion"], 1)
            self.assertGreater(result["model_rows"]["int_engagement_observation_latest"], 0)

    def test_repeatable_metadata_and_input_identity(self):
        configs, digests = [], []
        for reverse in [False, True]:
            with fixture() as connection:
                if reverse:
                    connection.execute("CREATE OR REPLACE TABLE base.store_sales AS SELECT * FROM base.store_sales ORDER BY ss_ticket_number DESC,ss_item_sk DESC")
                configs.append(generate.prepare(connection, "base", seed=31, report_date=date(2002, 12, 31)))
                digests.append(connection.execute("SELECT bit_xor(md5_number(to_json(t))) FROM ext_engagement_channel_history t").fetchone()[0])
        self.assertEqual(configs[0]["dataset_id"], configs[1]["dataset_id"])
        self.assertEqual(digests[0], digests[1])
        with fixture() as connection:
            connection.execute("UPDATE base.store_sales SET ss_quantity=9 WHERE ss_ticket_number=999")
            config = generate.prepare(connection, "base", seed=31, report_date=date(2002, 12, 31))
            self.assertNotEqual(configs[0]["dataset_id"], config["dataset_id"])

    def test_calibration_targets_and_declared_scope(self):
        with fixture() as connection:
            config = generate.prepare(connection, "base", "calibrated", report_date=date(2002, 12, 31))
            targets = {entry["metric"]: entry["value"] for entry in json.loads((generate.HERE / "profiles.json").read_text())["measured_reference_targets"]}
            entities, events = connection.execute("SELECT (SELECT count(*) FROM xt_engagements), (SELECT count(*) FROM xt_status_events)").fetchone()
            self.assertEqual(events, int(entities * targets["status_events_per_engagement_collection_key"]))
            actual, expected = connection.execute("SELECT count(*) FILTER(WHERE eligible_relationship), ceil(count(*) * ?) FROM xt_engagements", [targets["purchase_relationship_metadata_fraction"]]).fetchone()
            self.assertEqual(actual, expected)
            self.assertEqual(len(config["implemented_calibration_targets"]), 4)
            calibration = generate.statistics(connection)["calibration"]
            self.assertTrue(calibration["status_events_per_engagement_collection_key"]["implemented"])
            self.assertIsNone(calibration["parent_capacity_limit_pearson_r"]["observed"])
            self.assertFalse(calibration["parent_capacity_limit_pearson_r"]["implemented"])

    def test_equal_timestamp_observations_have_deterministic_winners(self):
        for reverse in [False, True]:
            with fixture() as connection:
                generate.prepare(connection, "base", report_date=date(2002, 12, 31))
                pipeline.build(connection, ["int_engagement_observation_latest"])
                path, parent = connection.execute("SELECT engagement_path,customer_path FROM int_engagement_observation_latest LIMIT 1").fetchone()
                values = [3, 9] if not reverse else [9, 3]
                for units in values:
                    connection.execute("INSERT INTO ext_engagement_observation_events VALUES (?,TIMESTAMP '2002-12-31 23:59:59',?,?,?,'retail','200212')", [str(units), path, parent, str(units)])
                pipeline.build(connection, ["int_engagement_observation_latest"])
                self.assertEqual(connection.execute("SELECT engagement_reported_units FROM int_engagement_observation_latest WHERE engagement_path=?", [path]).fetchone()[0], 9)

    def test_hot_cold_predicates_expose_status_skew(self):
        with fixture() as connection:
            generate.prepare(connection, "base", "jcc_stress", report_date=date(2002, 12, 31))
            total, reassigned, hot_keys = connection.execute("SELECT count(*), count(*) FILTER(WHERE origin='synthetic_jcc_assignment'), count(DISTINCT engagement_registry_key) FILTER(WHERE origin='synthetic_jcc_assignment') FROM xt_status_events").fetchone()
            self.assertEqual(reassigned, total//4)
            self.assertEqual(hot_keys, 5)
            cold = pipeline.build(connection, pipeline.PILOT_TARGETS, "cold")
            pipeline.validate(connection, cold["model_rows"])
            hot = pipeline.build(connection, pipeline.PILOT_TARGETS, "hot")
            pipeline.validate(connection, hot["model_rows"])
            self.assertGreater(hot["model_rows"]["int_hist_active_candidates_28d"], cold["model_rows"]["int_hist_active_candidates_28d"])
            eligible_hot_keys = connection.execute("""SELECT count(DISTINCT e.engagement_registry_key)
                FROM int_hist_eligible_status e JOIN xt_status_events s USING(engagement_registry_key)
                WHERE s.origin='synthetic_jcc_assignment'""").fetchone()[0]
            self.assertEqual(eligible_hot_keys, 5)
            households = connection.execute("SELECT household_type,count(*) FROM dim_household WHERE household_type IN ('hot','cold') GROUP BY 1").fetchall()
            self.assertLessEqual(abs(dict(households)["hot"]-dict(households)["cold"]), 1)

    def test_leading_null_channel_history_uses_full_frame(self):
        with fixture() as connection:
            generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            key, status = connection.execute("SELECT assessment_key,store_eligibility FROM ext_engagement_channel_history WHERE is_current AND store_eligibility IS NOT NULL LIMIT 1").fetchone()
            connection.execute("""INSERT INTO ext_engagement_channel_history
                SELECT 'new-null',assessment_key,NULL,catalog_eligibility,web_eligibility,
                       ingestion_time+INTERVAL 1 DAY,effective_start+INTERVAL 1 DAY,is_current,region_partition
                FROM ext_engagement_channel_history WHERE assessment_key=? AND is_current""", [key])
            pipeline.build(connection, ["int_channel_status_latest"])
            self.assertEqual(connection.execute("SELECT store_eligibility FROM int_channel_status_latest WHERE assessment_key=?", [key]).fetchall(), [(status,)])

    def test_latest_observation_insertion_and_retraction(self):
        with fixture() as connection:
            generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            pipeline.build(connection, pipeline.PILOT_TARGETS)
            registry_key, old_key = connection.execute("SELECT engagement_registry_key,assessment_key FROM int_hist_active_28d WHERE report_as_of_date=DATE '2002-12-31' LIMIT 1").fetchone()
            connection.execute("""INSERT INTO ext_engagement_status_events
                SELECT 'new-observation',engagement_registry_key,'20021231','86399',TIMESTAMP '2002-12-31 23:59:59',
                       TIMESTAMP '2003-01-01 00:59:59','new-assessment','20021231','200212','retail'
                FROM ext_engagement_status_events WHERE engagement_registry_key=? LIMIT 1""", [registry_key])
            result = pipeline.build(connection, pipeline.PILOT_TARGETS + ["int_pit_active_28d"])
            pipeline.validate(connection, result["model_rows"])
            self.assertEqual(connection.execute("SELECT assessment_key FROM int_hist_active_28d WHERE engagement_registry_key=? AND report_as_of_date=DATE '2002-12-31'", [registry_key]).fetchone()[0], "new-assessment")
            self.assertEqual(connection.execute("SELECT assessment_key FROM int_pit_active_28d WHERE engagement_registry_key=?", [registry_key]).fetchone()[0], "new-assessment")
            connection.execute("DELETE FROM ext_engagement_status_events WHERE _source_row_id='new-observation'")
            result = pipeline.build(connection, pipeline.PILOT_TARGETS)
            pipeline.validate(connection, result["model_rows"])
            self.assertEqual(connection.execute("SELECT assessment_key FROM int_hist_active_28d WHERE engagement_registry_key=? AND report_as_of_date=DATE '2002-12-31'", [registry_key]).fetchone()[0], old_key)

    def test_hidden_monthly_grain_preserves_equal_projected_rows(self):
        with fixture() as connection:
            generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            pipeline.build(connection, pipeline.PILOT_TARGETS)
            before = connection.execute("SELECT count(*) FROM fact_purchase_quantity_monthly_snapshot").fetchone()[0]
            connection.execute("""INSERT INTO int_purchase_usage_transaction
                SELECT event_time,'/engagements/additional-business-id',price_catalog_id,household_key,
                       account_offer_key,account_usage_key,platform_key,preference_key,product_tier_key,
                       product_version_key,engagement_type_key,price_catalog_key,purchase_quantity
                FROM int_purchase_usage_transaction LIMIT 1""")
            config = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
            connection.execute("CREATE OR REPLACE TABLE fact_purchase_quantity_monthly_snapshot AS " + pipeline.render_model("fact_purchase_quantity_monthly_snapshot", config))
            after = connection.execute("SELECT count(*) FROM fact_purchase_quantity_monthly_snapshot").fetchone()[0]
            self.assertEqual(after, before+1)
            pipeline.validate(connection, {"fact_purchase_quantity_monthly_snapshot": after})


if __name__ == "__main__":
    unittest.main()
