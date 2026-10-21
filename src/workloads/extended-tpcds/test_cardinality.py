"""Temporal cardinality contracts, independent of SQL and engine execution."""

import csv
import importlib.util
import json
import random
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("cardinality", HERE / "cardinality.py")
cardinality = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cardinality)


class CardinalityTests(unittest.TestCase):
    def test_duplicate_events_and_overlapping_windows(self):
        result = cardinality.count_windows(
            cardinality.read_events(HERE / "fixtures/eligible-events.csv"),
            date(2002, 1, 2), date(2002, 1, 4), [1, 2, 3],
        )
        self.assertEqual(result["report_date_rows"], 3)
        self.assertEqual(result["report_window_date_map_rows"], 9)
        for width, candidates, selected in [(1, 2, 2), (2, 6, 5), (3, 9, 6)]:
            counts = result["windows"][str(width)]
            self.assertEqual(counts["candidate_rows"], candidates)
            self.assertEqual(counts["selected_entity_dates"], selected)
            self.assertEqual(counts["unpivot_rows_without_enrichment_fanout"], 3 * selected)

    def test_matches_explicit_report_date_join(self):
        # An independent row-by-row join oracle checks interval union, including
        # dates outside the report range, gaps, duplicates and leap day.
        rng = random.Random(17)
        first, last = date(2000, 2, 25), date(2000, 3, 5)
        events = [(str(rng.randrange(8)), first + timedelta(days=rng.randrange(-30, 20)))
                  for _ in range(100)]
        for width in [1, 7, 28]:
            candidates = [(first + timedelta(days=i), entity)
                          for i in range((last - first).days + 1)
                          for entity, day in events
                          if 0 <= (first + timedelta(days=i) - day).days < width]
            counts = cardinality.count_windows(events, first, last, [width])["windows"][str(width)]
            self.assertEqual(counts["candidate_rows"], len(candidates))
            self.assertEqual(counts["selected_entity_dates"], len(set(candidates)))

    def test_empty_input(self):
        result = cardinality.count_windows([], date(2002, 1, 1), date(2002, 1, 1), [28])
        self.assertEqual(result["windows"]["28"]["candidate_rows"], 0)
        self.assertEqual(result["windows"]["28"]["candidate_rows_per_input_event"], 0)

    def test_invalid_inputs(self):
        first = date(2002, 1, 2)
        for widths in [[], [0], [-1], [1.5], [True], [1, True]]:
            with self.assertRaises(ValueError):
                cardinality.count_windows([], first, first, widths)
        with self.assertRaises(ValueError):
            cardinality.count_windows([], first, date(2002, 1, 1), [1])
        with self.assertRaises(ValueError):
            cardinality.count_windows([(" ", first)], first, first, [1])

    def test_invalid_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            for content in ["engagement_registry_key\na\n",
                            "engagement_registry_key,effective_date\na,invalid\n"]:
                path.write_text(content)
                with self.assertRaises(ValueError):
                    list(cardinality.read_events(path))

    def test_mapping_is_complete_and_acyclic(self):
        with (HERE / "model-mapping.csv").open(newline="") as handle:
            models = list(csv.DictReader(handle))
        self.assertEqual(len(models), 96)
        targets = {row["target_model"] for row in models}
        self.assertEqual(len(targets), 96)
        self.assertEqual(len({row["source_model"] for row in models}), 96)
        layers = {layer: sum(row["layer"] == layer for row in models)
                  for layer in ["staging", "dimensions", "intermediate", "facts", "metadata"]}
        self.assertEqual(layers, {"staging": 18, "dimensions": 11, "intermediate": 43,
                                  "facts": 23, "metadata": 1})
        graph = {row["target_model"]: row["parent_models"].split(";")
                 if row["parent_models"] else [] for row in models}
        visited, visiting = set(), set()

        def visit(name):
            self.assertIn(name, targets)
            self.assertNotIn(name, visiting)
            if name in visited:
                return
            visiting.add(name)
            for parent in graph[name]:
                visit(parent)
            visiting.remove(name)
            visited.add(name)

        for name in graph:
            visit(name)
        metadata = next(row for row in models if row["layer"] == "metadata")
        self.assertEqual(len(graph[metadata["target_model"]]), 4)
        for row in models:
            self.assertTrue(row["grain"])
            self.assertTrue(row["cardinality_rule"])
            self.assertGreaterEqual(int(row["original_seed_rows"]), 1)

    def test_stress_settings_are_labeled_as_assumptions(self):
        profiles = json.loads((HERE / "profiles.json").read_text())
        stress = profiles["profiles"]["jcc_stress"]
        self.assertEqual(stress["origin"], "chosen_stress_scenario")
        self.assertEqual(stress["hot_keys"], 5)
        self.assertEqual(stress["hot_event_share"], 0.25)
        self.assertEqual(profiles["profiles"]["native"]["transform_base_rows"], False)


if __name__ == "__main__":
    unittest.main()
