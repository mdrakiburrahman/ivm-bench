"""Exercise schedules, exact replay, bag semantics and transaction guards."""

import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

import benchmark
import generate
import mutations
from test_workload import fixture


class MutationTests(unittest.TestCase):
    def test_export_compression_preserves_types_nulls_duplicates_and_order(self):
        with tempfile.TemporaryDirectory() as folder, duckdb.connect() as connection:
            root = Path(folder)
            connection.execute("""CREATE TABLE input AS SELECT * FROM (VALUES
                ('b', 12.34::DECIMAL(10,2), TIMESTAMP '2002-12-31 12:34:56.123456', TRUE, 'café'),
                ('a', 12.34::DECIMAL(10,2), TIMESTAMP '2002-12-31 12:34:56.123456', TRUE, 'café'),
                ('c', NULL, NULL, NULL, NULL)
            ) t(_source_row_id, amount, event_time, flag, payload)""")
            image = mutations.export_rows(connection, 'SELECT * FROM input', root / 'rows.parquet', root)
            expected = connection.execute('SELECT * FROM input ORDER BY _source_row_id').fetchall()
            expected_types = connection.description
            actual = connection.execute('SELECT * FROM read_parquet(?)', [str(root / image['path'])]).fetchall()
            self.assertEqual(actual, expected)
            self.assertEqual(connection.description, expected_types)
            self.assertEqual(image['rows'], 3)
            self.assertEqual(image['sha256'], mutations.file_checksum(root / image['path']))
            self.assertEqual(connection.execute('SELECT DISTINCT compression FROM parquet_metadata(?)', [str(root / image['path'])]).fetchall(), [('ZSTD',)])

    def test_read_only_insert_plans_match_mutable_plans_without_changing_sources(self):
        for mode in ('insertion', 'repeated_insertions'):
            for initial, cohort in [('0', 'all'), ('50', 'hot'), ('100', 'cold')]:
                with self.subTest(mode=mode, initial=initial, cohort=cohort), tempfile.TemporaryDirectory() as folder:
                    manifests = []
                    for compact in (False, True):
                        with fixture() as connection:
                            generate.prepare(connection, 'base', report_date=date(2002, 12, 31), source_views=compact)
                            before = generate.statistics(connection)
                            step = {'insert_pct':'30', 'update_pct':'0', 'delete_pct':'0'}
                            manifests.append(mutations.plan(connection, Path(folder) / str(compact), mode=mode,
                                                            initial_pct=initial, steps=[step, step],
                                                            mutation_cohort=cohort, mutate_source=not compact))
                            if compact:
                                self.assertEqual(generate.statistics(connection), before)
                                self.assertEqual(connection.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'pool_%' OR table_name='changes'").fetchone()[0], 0)
                    # Parquet encodings may differ between a stored table and a
                    # projection. Compare schemas, ordered rows and plan metadata.
                    with duckdb.connect() as check:
                        for path in (Path(folder) / 'False').rglob('*.parquet'):
                            other = Path(folder) / 'True' / path.relative_to(Path(folder) / 'False')
                            expected = check.execute('SELECT * FROM read_parquet(?) ORDER BY _source_row_id', [str(path)]).fetchall()
                            schema = check.description
                            actual = check.execute('SELECT * FROM read_parquet(?) ORDER BY _source_row_id', [str(other)]).fetchall()
                            self.assertEqual(check.description, schema, str(path))
                            self.assertEqual(actual, expected, str(path))
                    without_encoding = lambda value: json.loads(json.dumps(value), object_hook=lambda obj: {k:v for k,v in obj.items() if k != 'sha256'})
                    self.assertEqual(without_encoding(manifests[0]), without_encoding(manifests[1]))

    def test_read_only_planning_rejects_modes_requiring_source_mutations(self):
        for mode in ('mixed', 'updates', 'deletes', 'days'):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, 'insert-only'):
                mutations.plan(None, 'unused', mode=mode, mutate_source=False)

    def test_cli_imports_existing_experiment_percentages_and_checks_scale(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as connection:
            root = Path(folder)
            generate.prepare(connection, "base", report_date=date(2002, 12, 31), base_scale_factor=1)
            connection.execute("ATTACH " + generate.quote(root / "input.duckdb") + " AS exported")
            connection.execute("COPY FROM DATABASE memory TO exported")
            connection.execute("DETACH exported")
            experiment = {"scale_factor":1,"batch_1_pct":50,"batch_2_insert_pct":20,
                          "batch_2_update_pct":30,"batch_2_delete_pct":10,"batch_3_pct":40}
            path = root / "experiment.json"
            path.write_text(json.dumps({"experiments":[experiment]}))
            command = [sys.executable,str(mutations.HERE / "mutations.py"),"--database",str(root / "input.duckdb"),
                       "--output-dir",str(root / "plan"),"--mode","mixed","--experiment",str(path)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads((root / "plan/manifest.json").read_text())
            self.assertEqual(manifest["settings"]["initial_pct"], "50")
            self.assertEqual(manifest["settings"]["steps"][0], {"insert_pct":20,"update_pct":30,"delete_pct":10})
            self.assertEqual(manifest["settings"]["steps"][1]["insert_pct"], 40)
            experiment["scale_factor"] = 2
            path.write_text(json.dumps(experiment))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scale factor differs", result.stderr)

    def test_extending_schedule_preserves_existing_arrival_identities(self):
        with tempfile.TemporaryDirectory() as folder:
            keys = []
            for rounds in [1, 3]:
                with fixture() as connection:
                    generate.prepare(connection, "base", report_date=date(2002, 12, 31))
                    step = {"insert_pct":"30","update_pct":"20","delete_pct":"10"}
                    root = Path(folder) / str(rounds)
                    manifest = mutations.plan(connection, root, mode="mixed", steps=[step]*rounds)
                    image = next(op["after"] for op in manifest["batches"][0]["operations"] if op["table"]=="ext_purchase_usage_events" and op["kind"]=="insert")
                    keys.append(connection.execute("SELECT _source_row_id FROM read_parquet(?) ORDER BY _source_row_id", [str(root/image["path"])]).fetchall())
            self.assertEqual(keys[0], keys[1])

    def test_all_modes_recompute_and_validate_every_model_after_every_batch(self):
        for mode in mutations.MODES:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                with fixture() as planner:
                    generate.prepare(planner, "base", report_date=date(2002, 12, 31))
                    percentages = {"insert_pct": "20" if mode in ("insertion", "repeated_insertions", "mixed") else "0",
                                   "update_pct": "30" if mode in ("updates", "mixed") else "0",
                                   "delete_pct": "20" if mode in ("deletes", "mixed") else "0"}
                    manifest = mutations.plan(planner, root / "plan", mode=mode,
                                              steps=[dict(percentages) for _ in range(3 if mode == "days" else 2)])
                with contextlib.redirect_stdout(io.StringIO()):
                    results = benchmark.run(root / "plan/manifest.json", root / "run")
                self.assertEqual(len(results["batches"]), 4 if mode == "days" else 3)
                for batch in results["batches"]:
                    self.assertEqual(batch["reference_check"]["models_checked"], 96)
                    self.assertEqual(len(batch["cardinality_checks"]), 10)
                    self.assertEqual(batch["configuration"]["report_date"], "2002-12-31")
                self.assertEqual(results["batches"][-1]["source_rows"], manifest["batches"][-1]["source_rows"])

    def test_replay_matches_planned_source_payloads(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as planner, duckdb.connect() as replay:
            generate.prepare(planner, "base", report_date=date(2002, 12, 31))
            step = {"insert_pct": "40", "update_pct": "30", "delete_pct": "20"}
            manifest = mutations.plan(planner, Path(folder) / "plan", mode="mixed", steps=[step, step])
            mutations.load_initial(replay, manifest, Path(folder) / "plan")
            for batch in manifest["batches"]:
                mutations.apply_batch(replay, manifest, Path(folder) / "plan", batch)
            for table in mutations.sources():
                planned = planner.execute(f"SELECT * FROM {table} ORDER BY _source_row_id").fetchall()
                observed = replay.execute(f"SELECT * FROM {table} ORDER BY _source_row_id").fetchall()
                self.assertEqual(planned, observed, table)

    def test_empty_initial_state_and_full_delete_keep_all_models_valid(self):
        scenarios = [
            ("insertion", "0", {"insert_pct":"100","update_pct":"0","delete_pct":"0"}),
            ("deletes", "100", {"insert_pct":"0","update_pct":"0","delete_pct":"100"}),
        ]
        for mode, initial, step in scenarios:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as folder, fixture() as connection:
                generate.prepare(connection, "base", report_date=date(2002, 12, 31))
                root = Path(folder)
                mutations.plan(connection, root / "plan", mode=mode, initial_pct=initial, steps=[step])
                with contextlib.redirect_stdout(io.StringIO()):
                    results = benchmark.run(root / "plan/manifest.json", root / "run")
                empty_batch = results["batches"][0 if mode == "insertion" else 1]
                self.assertEqual(empty_batch["model_rows"]["fact_purchase_quantity_monthly_snapshot"], 0)
                self.assertEqual(empty_batch["model_rows"]["int_hist_active_candidates_28d"], 0)

    def test_image_corruption_and_wrong_plan_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as planner, duckdb.connect() as replay:
            generate.prepare(planner, "base", report_date=date(2002, 12, 31))
            root = Path(folder) / "plan"
            manifest = mutations.plan(planner, root)
            mutations.load_initial(replay, manifest, root)
            wrong = {**manifest, "plan_id":"another-plan"}
            with self.assertRaisesRegex(ValueError, "does not match"):
                mutations.apply_batch(replay, wrong, root, manifest["batches"][0])
            image = manifest["batches"][0]["operations"][0]["after"]
            path = root / image["path"]
            path.write_bytes(path.read_bytes() + b"corrupted")
            with self.assertRaisesRegex(ValueError, "checksum"):
                mutations.apply_batch(replay, manifest, root, manifest["batches"][0])
            self.assertEqual(replay.execute("SELECT number FROM xt_applied_batch").fetchone()[0], 1)

    def test_repeated_insertions_keep_payloads_but_use_new_physical_keys(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as planner, duckdb.connect() as replay:
            generate.prepare(planner, "base", report_date=date(2002, 12, 31))
            step = {"insert_pct": "100", "update_pct": "0", "delete_pct": "0"}
            manifest = mutations.plan(planner, Path(folder) / "plan", mode="repeated_insertions", steps=[step] * 4)
            mutations.load_initial(replay, manifest, Path(folder) / "plan")
            initial_count, initial_sum = replay.execute("SELECT count(*),sum(purchase_quantity) FROM ext_purchase_usage_events").fetchone()
            previous_keys = set()
            for round_number, batch in enumerate(manifest["batches"], 2):
                image = next(op["after"] for op in batch["operations"] if op["table"] == "ext_purchase_usage_events")
                file = Path(folder) / "plan" / image["path"]
                inserted_keys = {row[0] for row in replay.execute("SELECT _source_row_id FROM read_parquet(?)", [str(file)]).fetchall()}
                self.assertFalse(previous_keys & inserted_keys)
                previous_keys |= inserted_keys
                difference = replay.execute("SELECT count(*) FROM ((SELECT * EXCLUDE(_source_row_id) FROM read_parquet(?) EXCEPT ALL SELECT * EXCLUDE(_source_row_id) FROM read_parquet(?)) UNION ALL (SELECT * EXCLUDE(_source_row_id) FROM read_parquet(?) EXCEPT ALL SELECT * EXCLUDE(_source_row_id) FROM read_parquet(?)))", [str(file),str(Path(folder)/"plan"/manifest["initial"]["ext_purchase_usage_events"]["path"])]*2).fetchone()[0]
                self.assertEqual(difference, 0)
                mutations.apply_batch(replay, manifest, Path(folder) / "plan", batch)
                count, total = replay.execute("SELECT count(*),sum(purchase_quantity) FROM ext_purchase_usage_events").fetchone()
                self.assertEqual(count, initial_count * round_number)
                self.assertEqual(total, initial_sum * round_number)

    def test_days_release_original_rows_once_and_do_not_move_the_clock(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as planner:
            generate.prepare(planner, "base", report_date=date(2002, 12, 31))
            originals = {table: {row[0] for row in planner.execute(f"SELECT _source_row_id FROM {table} WHERE {day} <= DATE '2002-12-31'").fetchall()} for table, day in mutations.EVENT_DATES.items()}
            manifest = mutations.plan(planner, Path(folder) / "plan", mode="days", days=3)
            for table, day in mutations.EVENT_DATES.items():
                seen = {row[0] for row in planner.execute("SELECT _source_row_id FROM read_parquet(?)", [str(Path(folder)/"plan"/manifest["initial"][table]["path"])]).fetchall()}
                for batch in manifest["batches"]:
                    image = next(op["after"] for op in batch["operations"] if op["table"] == table)
                    path = str(Path(folder)/"plan"/image["path"])
                    incoming = planner.execute(f"SELECT _source_row_id,{day} FROM read_parquet(?)", [path]).fetchall()
                    self.assertTrue(all(str(event_day) == batch["arrival_date"] for _, event_day in incoming))
                    keys = {key for key, _ in incoming}
                    self.assertFalse(seen & keys)
                    seen |= keys
                self.assertEqual(seen, originals[table])
            self.assertEqual(manifest["settings"]["clock"], "fixed_report_date")

    def test_percentages_are_validated_before_writing(self):
        for value in ["NaN", "Infinity", "-1", "101", "garbage"]:
            with self.assertRaises(ValueError):
                mutations.percentage(value)
        with tempfile.TemporaryDirectory() as folder, fixture() as connection:
            generate.prepare(connection, "base", report_date=date(2002, 12, 31))
            path = Path(folder) / "plan"
            with self.assertRaisesRegex(ValueError, "sum"):
                mutations.plan(connection, path, mode="mixed", steps=[{"insert_pct":"0","update_pct":"60","delete_pct":"60"}])
            self.assertFalse(path.exists())

    def test_failed_batch_rolls_back_and_replayed_batch_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as planner, duckdb.connect() as replay:
            generate.prepare(planner, "base", report_date=date(2002, 12, 31))
            manifest = mutations.plan(planner, Path(folder) / "plan", mode="updates", steps=[{"insert_pct":"0","update_pct":"100","delete_pct":"0"}])
            mutations.load_initial(replay, manifest, Path(folder) / "plan")
            total = replay.execute("SELECT sum(purchase_quantity) FROM ext_purchase_usage_events").fetchone()[0]
            # A later feed fails after the purchase update would already run.
            replay.execute("UPDATE ext_customer_status_events SET customer_status='unexpected'")
            with self.assertRaisesRegex(ValueError, "before-image"):
                mutations.apply_batch(replay, manifest, Path(folder) / "plan", manifest["batches"][0])
            self.assertEqual(replay.execute("SELECT sum(purchase_quantity) FROM ext_purchase_usage_events").fetchone()[0], total)
            self.assertEqual(replay.execute("SELECT number FROM xt_applied_batch").fetchone()[0], 1)
            # Restore from the initial image and retry the same atomic batch.
            path = str(Path(folder)/"plan"/manifest["initial"]["ext_customer_status_events"]["path"])
            replay.execute("CREATE OR REPLACE TABLE ext_customer_status_events AS SELECT * FROM read_parquet(?)", [path])
            mutations.apply_batch(replay, manifest, Path(folder) / "plan", manifest["batches"][0])
            with self.assertRaisesRegex(ValueError, "once"):
                mutations.apply_batch(replay, manifest, Path(folder) / "plan", manifest["batches"][0])

    def test_plan_identity_includes_modes_batches_and_bindings(self):
        with tempfile.TemporaryDirectory() as folder:
            identifiers = []
            for index, settings in enumerate([{}, {"mode":"updates"}, {"mutation_cohort":"hot"}, {"seed":31}]):
                with fixture() as connection:
                    generate.prepare(connection, "base", report_date=date(2002, 12, 31))
                    manifest = mutations.plan(connection, Path(folder) / str(index), **settings)
                    identifiers.append(manifest["plan_id"])
            self.assertEqual(len(set(identifiers)), 4)

    def test_mutation_cohort_leaves_other_households_unchanged(self):
        with tempfile.TemporaryDirectory() as folder, fixture() as connection:
            generate.prepare(connection, "base", "jcc_stress", report_date=date(2002, 12, 31))
            cold = {table: connection.execute(f"SELECT * FROM {table} WHERE {mutations.cohort_predicate(table, 'cold')} ORDER BY _source_row_id").fetchall() for table in mutations.UPDATES}
            step = {"insert_pct":"100","update_pct":"100","delete_pct":"0"}
            manifest = mutations.plan(connection, Path(folder) / "plan", mode="mixed", steps=[step], mutation_cohort="hot")
            self.assertEqual(manifest["settings"]["mutation_cohort"], "hot")
            for table, expected in cold.items():
                observed = connection.execute(f"SELECT * FROM {table} WHERE {mutations.cohort_predicate(table, 'cold')} ORDER BY _source_row_id").fetchall()
                self.assertEqual(expected, observed, table)


if __name__ == "__main__":
    unittest.main()
