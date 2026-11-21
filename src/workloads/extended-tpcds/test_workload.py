"""Execute the workload over small TPC-DS-shaped inputs, without downloads."""

import json
import hashlib
import math
import tempfile
import unittest
from decimal import Decimal
from datetime import date, datetime, timedelta

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
    def test_binary_key_count_with_small_budget_retains_null_and_duplicate_semantics(self):
        with tempfile.TemporaryDirectory() as folder, duckdb.connect(config={
            'memory_limit':'64MB', 'threads':1, 'temp_directory':folder,
            'max_temp_directory_size':'1GB',
        }) as connection:
            identities = ["md5(n::VARCHAR)::UUID",
                          "('0000'||"+generate.purchase_identity("'store'",'n//2','n%2+1')+")::UUID"]
            for identity in identities:
                with self.subTest(identity=identity):
                    counts = generate.key_counts(connection, f"""SELECT {identity} AS key FROM (
                        SELECT n FROM range(2000000) t(n) UNION ALL SELECT 0
                        UNION ALL SELECT 1 UNION ALL SELECT NULL::BIGINT) rows""")
                    self.assertEqual(counts, (2000003, 2000000))

    def test_structured_identity_boundaries_do_not_overlap_or_truncate(self):
        with duckdb.connect() as connection:
            identities = []
            for family in (0, 1, 65535):
                for ordinal in (0, 1, 2**64-1):
                    for suffix in (0, 1, 2**32-1):
                        token = connection.execute('SELECT lower('+generate.row_identity(family,ordinal,suffix)+')').fetchone()[0]
                        self.assertEqual(token,f'{family:04x}{ordinal:016x}{suffix:08x}')
                        identities.append('0012'+token)
            self.assertEqual(len(set(identities)),len(identities))
            for parts in [('NULL',0,0),(0,'NULL',0),(0,0,'NULL')]:
                self.assertIsNone(connection.execute('SELECT '+generate.row_identity(*parts)).fetchone()[0])
            for family,ordinal,suffix in [(-1,0,0),(65536,0,0),(0,-1,0),(0,2**64,0),(0,0,-1),(0,0,2**32)]:
                with self.subTest(components=(family,ordinal,suffix)), self.assertRaises(duckdb.Error):
                    connection.execute('SELECT '+generate.row_identity(family,ordinal,suffix)).fetchone()

    def test_normalized_line_identity_validation_rejects_duplicates_and_null_components(self):
        for fault in ('duplicate','null_order','null_item'):
            with self.subTest(fault=fault), fixture() as connection:
                if fault=='duplicate':
                    connection.execute('INSERT INTO base.store_sales SELECT * FROM base.store_sales LIMIT 1')
                elif fault=='null_order':
                    connection.execute('UPDATE base.store_sales SET ss_ticket_number=NULL WHERE ss_ticket_number=0')
                else:
                    connection.execute('UPDATE base.store_sales SET ss_item_sk=NULL WHERE ss_ticket_number=0')
                with self.assertRaisesRegex(ValueError,'sales line identities are not unique'):
                    generate.prepare(connection,'base',source_views=True,report_date=date(2002,12,31))

    def test_compact_engagements_match_original_ranking_including_hash_collisions(self):
        for profile, collide in [('native',False), ('calibrated',False), ('native',True)]:
            with self.subTest(profile=profile, collide=collide), fixture() as connection:
                if collide:
                    def test_md5(value):
                        return '0'*32 if value.startswith('17:/accounts/') else hashlib.md5(value.encode()).hexdigest()
                    connection.create_function('test_md5', test_md5, ['VARCHAR'], 'VARCHAR')
                    connection.execute('CREATE MACRO md5(value) AS test_md5(value)')
                config = generate.prepare(connection, 'base', profile=profile, report_date=date(2002,12,31))
                reference = connection.execute("""SELECT *, row_number() OVER (
                    ORDER BY md5('17:' || engagement_path), engagement_path) AS engagement_number
                    FROM (SELECT engagement_path, engagement_registry_key, account_id, customer_path,
                        household_id, household_cohort, item_id,
                        min(purchase_line_id) AS origin_purchase_line_id,
                        min(event_time) AS first_event, max(event_time) AS last_event, count(*) AS purchase_lines
                        FROM xt_purchase GROUP BY ALL) e
                    ORDER BY engagement_number""").fetchall()
                eligible = (math.ceil(len(reference)*config['calibration_target_values']['purchase_relationship_metadata_fraction'])
                            if profile=='calibrated' else len(reference))
                expected = [(*row, row[-1]<=eligible) for row in reference]
                actual = connection.execute('SELECT * FROM xt_engagements ORDER BY engagement_number').fetchall()
                self.assertEqual(actual, expected)
                self.assertEqual(connection.execute("SELECT count(*) FROM information_schema.columns WHERE table_name='xt_engagement_keys' AND column_name IN ('engagement_path','engagement_registry_key','account_id','customer_path')").fetchone()[0], 0)

    def test_source_projections_preserve_tables_keys_and_statistics(self):
        for profile in ('native', 'calibrated', 'jcc_stress'):
            with self.subTest(profile=profile), fixture() as tables, fixture() as views:
                expected = generate.prepare(tables, 'base', profile=profile, report_date=date(2002, 12, 31))
                actual = generate.prepare(views, 'base', profile=profile, report_date=date(2002, 12, 31), source_views=True)
                self.assertEqual(actual, expected)
                self.assertEqual(views.execute('SELECT * FROM normalized_purchase ORDER BY purchase_line_id').fetchall(),
                                 tables.execute('SELECT * FROM normalized_purchase ORDER BY purchase_line_id').fetchall())
                self.assertEqual(generate.statistics(views), generate.statistics(tables))
                stats = generate.statistics(views)
                rows, keys = views.execute('SELECT count(*), count(DISTINCT engagement_registry_key) FROM xt_status_events').fetchone()
                self.assertEqual((stats['status_event_rows'], stats['status_registry_keys']), (rows,keys))
                cohorts = views.execute("""SELECT e.household_cohort, count(*), count(DISTINCT s.engagement_registry_key)
                    FROM xt_status_events s JOIN xt_engagements e USING (engagement_registry_key)
                    GROUP BY 1 ORDER BY 1""").fetchall()
                self.assertEqual(stats['cohorts'], [{'cohort':c,'events':n,'registry_keys':k} for c,n,k in cohorts])
                for namespace, name in enumerate(json.loads((generate.HERE / 'source-schema.json').read_text()),1):
                    invalid = views.execute(f"SELECT count(*) FROM {name} WHERE _source_row_id IS NULL OR length(_source_row_id)<>32 OR substr(_source_row_id,1,4)<>'{namespace:04x}'").fetchone()[0]
                    self.assertEqual(invalid,0,name)
                    self.assertEqual(views.execute(f'SELECT * FROM {name} ORDER BY _source_row_id').fetchall(),
                                     tables.execute(f'SELECT * FROM {name} ORDER BY _source_row_id').fetchall(), name)
                self.assertEqual(views.execute("SELECT count(*) FROM information_schema.tables WHERE table_type='VIEW' AND table_name LIKE 'ext_%'").fetchone()[0], 18)

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

    def test_null_safe_household_joins_preserve_original_model_bags(self):
        with fixture() as connection:
            configuration = generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            pipeline.build(connection)
            # Null and zero identifiers must remain distinct, including duplicate inputs.
            for suffix in ("28d", "7d", "daily"):
                parent = "int_hist_unpivoted_" + suffix
                connection.execute(f"UPDATE {parent} SET household_id=NULL WHERE household_key=(SELECT min(household_key) FROM {parent})")
                connection.execute(f"INSERT INTO {parent} SELECT * REPLACE (0 AS household_id) FROM {parent} WHERE household_id IS NULL ORDER BY report_as_of_date LIMIT 1")
                connection.execute(f"INSERT INTO {parent} SELECT * FROM {parent} WHERE household_id IS NULL ORDER BY report_as_of_date LIMIT 1")
            self.assertGreater(connection.execute("SELECT count(*) FROM int_hist_unpivoted_28d WHERE household_id IS NULL").fetchone()[0], 0)
            checked = []
            predicate = "a.household_id IS NOT DISTINCT FROM m.household_id"
            original = "(a.household_id = m.household_id OR (a.household_id IS NULL AND m.household_id IS NULL))"
            for name in pipeline.mapping():
                sql = pipeline.render_model(name, configuration)
                if predicate not in sql:
                    continue
                connection.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
                reference = sql.replace(predicate, original)
                difference = connection.execute(f"""SELECT count(*) FROM (
                    (SELECT * FROM {name} EXCEPT ALL SELECT * FROM ({reference}))
                    UNION ALL
                    (SELECT * FROM ({reference}) EXCEPT ALL SELECT * FROM {name})
                )""").fetchone()[0]
                self.assertEqual(difference, 0, name)
                checked.append(name)
            self.assertEqual(len(checked), 9)

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
            density = targets["status_events_per_engagement_collection_key"]
            fraction = density-math.floor(density)
            self.assertLessEqual(abs(events-entities*density), 5*math.sqrt(entities*fraction*(1-fraction)))
            self.assertEqual(config['status_count_assignment'], 'independently_seeded_hash_threshold')
            actual, expected = connection.execute("SELECT count(*) FILTER(WHERE eligible_relationship), ceil(count(*) * ?) FROM xt_engagements", [targets["purchase_relationship_metadata_fraction"]]).fetchone()
            self.assertEqual(actual, expected)
            self.assertEqual(len(config["implemented_calibration_targets"]), 4)
            calibration = generate.statistics(connection)["calibration"]
            self.assertTrue(calibration["status_events_per_engagement_collection_key"]["implemented"])
            self.assertIsNone(calibration["parent_capacity_limit_pearson_r"]["observed"])
            self.assertFalse(calibration["parent_capacity_limit_pearson_r"]["implemented"])

    def test_compact_calibrated_events_preserve_ids_times_and_origins(self):
        with fixture() as connection:
            config = generate.prepare(connection, 'base', 'calibrated', report_date=date(2002,12,31))
            density = config['calibration_target_values']['status_events_per_engagement_collection_key']
            expected = []
            def fraction(value):
                return int(hashlib.md5(('17:'+value).encode()).hexdigest()[:8],16)/2**32
            for path, key, origin in connection.execute(
                'SELECT engagement_path, engagement_registry_key, origin_purchase_line_id FROM xt_engagements'
            ).fetchall():
                count = math.floor(density) + (fraction('status-count:'+path)<density-math.floor(density))
                for number in range(count):
                    timestamp = datetime(2002,12,31,14)-timedelta(days=math.floor(fraction(path+str(number))*365))
                    expected.append((f'synthetic:{key}:{number}',key,timestamp,origin,'synthetic_calibrated'))
            actual = connection.execute('SELECT * FROM xt_status_events ORDER BY event_id').fetchall()
            self.assertEqual(actual, sorted(expected))

    def test_compact_calibrated_events_reject_colliding_published_registry_keys(self):
        for validate in (True,False):
            with self.subTest(validate=validate), fixture() as connection:
                def registry_hash(value):
                    return '0'*32 if value.startswith('account:') else hashlib.md5(value.encode()).hexdigest()
                connection.create_function('registry_hash', registry_hash, ['VARCHAR'], 'VARCHAR')
                connection.execute('CREATE MACRO md5(value) AS registry_hash(value)')
                with self.assertRaisesRegex(ValueError, 'duplicate engagement registry identities'):
                    generate.prepare(connection, 'base', 'calibrated', report_date=date(2002,12,31), validate_source_ids=validate)

    def test_calibrated_extra_observations_are_not_clustered_at_first_ranks(self):
        with fixture() as connection:
            generate.prepare(connection, 'base', 'calibrated', report_date=date(2002,12,31))
            entities = connection.execute('SELECT count(*) FROM xt_engagements').fetchone()[0]
            counts = connection.execute(f"""SELECT e.engagement_number<={entities//2} AS first_half,
                    min(s.observations), max(s.observations)
                FROM xt_engagements e JOIN (
                    SELECT engagement_registry_key, count(*) AS observations
                    FROM xt_status_events GROUP BY engagement_registry_key
                ) s USING (engagement_registry_key)
                GROUP BY 1 ORDER BY 1""").fetchall()
            self.assertEqual(counts, [(False,3,4),(True,3,4)])

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

    def test_monthly_effective_units_round_half_away_from_zero(self):
        with duckdb.connect() as connection:
            connection.execute("""CREATE TABLE int_engagement_status_transaction AS
                SELECT TIMESTAMP '2002-12-01' AS event_time,
                       'engagement-' || household_key AS engagement_path,
                       household_key, 1 AS account_offer_key, 1 AS account_usage_key,
                       1 AS platform_key, 1 AS preference_key, 1 AS product_tier_key,
                       1 AS product_version_key, 1 AS engagement_type_key, 1 AS mode_key,
                       effective_units::DOUBLE AS effective_units
                FROM (VALUES (1, 0.49), (2, 0.5), (3, -0.5), (4, 1.5),
                             (5, -1.5), (6, NULL)) t(household_key, effective_units)""")
            sql = pipeline.render_model("fact_engagement_status_monthly_snapshot",
                                        {"report_date": "2002-12-31", "history_months": 12})
            connection.execute("CREATE TABLE snapshot AS " + sql)
            self.assertEqual(connection.execute(
                "SELECT household_key,effective_units_count FROM snapshot ORDER BY household_key"
            ).fetchall(), [(1, 0), (2, 1), (3, -1), (4, 2), (5, -2), (6, None)])

    def test_monthly_fractional_units_use_fixed_precision_before_summing(self):
        # These nearby DOUBLE values straddle a half-unit total depending on
        # reduction order; at ten decimal places each contributes 0.0384615385.
        values = [0.038461538461538464] * 12 + [0.03846153846153846]
        for reverse in (False, True):
            with self.subTest(reverse=reverse), duckdb.connect(config={"memory_limit": "128MB", "threads": 1}) as connection:
                connection.execute("""CREATE TABLE int_engagement_status_transaction AS
                    SELECT TIMESTAMP '2002-12-01' AS event_time,
                           'engagement-' || household_key || '-' || i AS engagement_path,
                           household_key, 1 AS account_offer_key, 1 AS account_usage_key,
                           1 AS platform_key, 1 AS preference_key, 1 AS product_tier_key,
                           1 AS product_version_key, 1 AS engagement_type_key, 1 AS mode_key,
                           units * sign AS effective_units
                    FROM (SELECT unnest(?::DOUBLE[]) AS units, unnest(range(13)) AS i)
                    CROSS JOIN (VALUES (1,1),(2,-1)) s(household_key,sign)""",
                    [values[::-1] if reverse else values])
                sql = pipeline.render_model("fact_engagement_status_monthly_snapshot",
                                            {"report_date": "2002-12-31", "history_months": 12})
                connection.execute("CREATE TABLE snapshot AS " + sql)
                self.assertEqual(connection.execute(
                    "SELECT household_key,effective_units_count FROM snapshot ORDER BY household_key"
                ).fetchall(), [(1, 1), (2, -1)])

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
