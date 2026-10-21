import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from handlers import chart, oat_chart


def experiment(refresh_count=20):
    return {
        "exp_idx": 0, "status": "completed",
        "inputs": {"scale_factor": 10, "repeated_refresh": True,
                   "refresh_count": refresh_count, "engines": ["spark", "spark-openivm"]},
        "engines": {
            engine: {"batches": [
                {"batch_num": n, "duration_s": n * multiplier, "status": "completed",
                 "extra": {"storage": {
                     "status": "ok", "visible_output_bytes": n * 1024,
                 }}}
                for n in range(1, refresh_count + 2)
            ]}
            for engine, multiplier in (("spark", 2), ("spark-openivm", 1))
        },
    }


class RepeatedRefreshChartsTest(unittest.TestCase):
    def test_markdown_keeps_final_round_timings_storage_and_ratio(self):
        exp = experiment()
        state = {"status": "completed", "experiments": [exp]}
        with patch.object(oat_chart, "_load_oat_state", return_value=state):
            md = oat_chart.generate_oat_results_md("test")
        self.assertIn("spark b21", md)
        self.assertIn("spark-openivm b21", md)
        self.assertIn("b21 ratio", md)
        storage = md.split("## Per-engine storage overhead")[1].split("## Per-engine ratio")[0]
        self.assertEqual(storage.count("| spark |"), 21)
        self.assertEqual(storage.count("| spark-openivm |"), 21)
        self.assertIn("| spark | 21 | ok | 21.00 KiB |", storage)
        timing = md.split("## Per-engine timing (aggregate)")[1].split("## Per-engine storage")[0]
        self.assertIn("00:42", timing)
        self.assertIn("00:21", timing)
        ratio = md.split("## Per-engine ratio (openivm / spark)")[1].split("## Per-model break-even")[0]
        rows = [line for line in ratio.splitlines() if line.startswith("|")]
        self.assertEqual(len(rows[0].split("|")), len(rows[-1].split("|")))
        self.assertEqual(rows[-1].split("|")[4:-1], [" 0.50 "] * 21)

    def test_mixed_sweeps_keep_expected_rounds_without_inventing_values(self):
        repeated = experiment()
        repeated["status"] = "skipped"
        repeated["engines"] = {}
        legacy = {"inputs": {"engines": ["spark"]}, "engines": {"spark": {"batches": [
            {"batch_num": 1, "duration_s": 1}, {"batch_num": 21, "duration_s": 21},
        ]}}}
        columns = oat_chart._aggregate_columns([legacy, repeated], ["spark"])
        outputs = [c for c in columns if c.key.startswith("spark.b")]
        self.assertEqual([c.key for c in outputs], [f"spark.b{n}" for n in range(1, 22)])
        values, _ = oat_chart._aggregate_row(legacy, outputs, 0)
        self.assertEqual(values[0], 1)
        self.assertIsNone(values[1])
        self.assertEqual(values[-1], 21)
        values, _ = oat_chart._aggregate_row(repeated, outputs, 1)
        self.assertTrue(all(v is None for v in values))
        self.assertEqual(oat_chart._batch_numbers([{"inputs": {}, "engines": {}}]), [1, 2, 3])

    def test_model_artifact_lookup_distinguishes_batch_1_from_10(self):
        exp = {"dbt_run_files": {"spark": ["run-spark-batch10.json", "run-spark-batch1.json"]}}
        self.assertTrue(oat_chart._dbt_run_path(exp, "spark", 1, "/tmp").endswith("-batch1.json"))
        exp["dbt_run_files"]["spark"] = ["run-spark-batch21.json"]
        self.assertTrue(oat_chart._dbt_run_path(exp, "spark", 1, "/tmp").endswith("-batch1.json"))

    def test_rendered_aggregate_and_model_charts_include_batch_21(self):
        exp = experiment()
        state = {"status": "completed", "experiments": [exp]}
        original = oat_chart._png_from_fig
        with patch.object(oat_chart, "_load_oat_state", return_value=state), \
                patch.object(oat_chart, "_png_from_fig", wraps=original) as save:
            aggregate = oat_chart.generate_oat_aggregate_png("test")
            fig = save.call_args.args[0]
            self.assertIn("spark.b21", [t.get_text() for t in fig.axes[0].texts])
            with patch.object(oat_chart, "_load_model_times", return_value={"model": ("model", 1.0)}) as load:
                per_model = oat_chart.generate_oat_per_model_png("test")
            self.assertIn(21, {call.args[2] for call in load.call_args_list})
            fig = save.call_args.args[0]
            titles = [ax.get_title() for ax in fig.axes]
            self.assertEqual(sum(t.startswith("batch ") for t in titles), 21)
            self.assertIn("batch 21: log₂(openivm / spark)", titles)
        self.assertTrue(aggregate.startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertTrue(per_model.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_scale_chart_keeps_later_source_stats_without_model_timings(self):
        import matplotlib.figure
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "benchmark-results.json").write_text(json.dumps({
                "engines": {"duckdb": {"batches": [
                    {"batch_num": n, "duration_s": n} for n in range(1, 5)
                ]}},
            }))
            Path(directory, "delta-stats-duckdb-batch4.json").write_text(json.dumps({
                "tables": [{"name": "trade", "rows": 10, "size_gb": 0.1}],
            }))
            original = matplotlib.figure.Figure.savefig
            figures = []

            def save(fig, *args, **kwargs):
                figures.append(fig)
                return original(fig, *args, **kwargs)

            with patch.object(matplotlib.figure.Figure, "savefig", save):
                png = chart.generate_chart_png(directory, sf=3)
            self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))
            self.assertTrue(any(ax.get_title().startswith("Batch 4 ") for ax in figures[-1].axes))


if __name__ == "__main__":
    unittest.main()
