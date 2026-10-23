"""A paired study changes only its immutable pin and uses balanced orders."""

import importlib.util
import csv
import json
from pathlib import Path
import tempfile
import unittest

PATH = Path(__file__).resolve().parents[4]/'tools/scripts/benchmarks/fig1_attribution.py'
spec = importlib.util.spec_from_file_location('fig1_attribution', PATH)
study = importlib.util.module_from_spec(spec)
spec.loader.exec_module(study)


class Fig1AttributionTest(unittest.TestCase):
    def test_cumulative_command_profiles_require_fresh_complete_model_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'dbt-server').mkdir()
            (root/'oat').mkdir()
            (root/'dbt-server/benchmark-results.json').write_text(json.dumps(
                {'status': 'completed', 'repetition_count': 1}))
            with (root/'oat/results.csv').open('w') as f:
                writer = csv.DictWriter(f, fieldnames=[
                    'batch_num', 'batch_status', 'batch_error', 'engine', 'scale_factor', 'openivm_validate'])
                writer.writeheader()
                writer.writerows(dict(batch_num=b, batch_status='completed', batch_error='',
                                      engine='spark-openivm', scale_factor=100, openivm_validate=False)
                                 for b in [1, 2, 3])
            rows = []
            for batch in [1, 2, 3]:
                rows.extend(dict(step_name='command_span', refresh_id=f'{batch}-{i}', view_name=f'm{i}')
                            for i in range(49))
                with (root/f'dbt-server/spark-openivm-profile-batch{batch}.csv').open('w') as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
            self.assertEqual(len(study.verify_results(root, 100, 'spark-openivm', False)), 3)
            # A stale export must not turn missing batch-three commands into a pass.
            (root/'dbt-server/spark-openivm-profile-batch3.csv').write_text(
                (root/'dbt-server/spark-openivm-profile-batch2.csv').read_text())
            with self.assertRaisesRegex(RuntimeError, 'batch 3'):
                study.verify_results(root, 100, 'spark-openivm', False)

    def test_pin_replacement_covers_build_and_runtime_only(self):
        text = 'ARG OPENIVM_COMMIT=' + 'a'*40 + '\nKEEP=123\nARG OPENIVM_COMMIT=' + 'a'*40 + '\n'
        result = study.replace_pin(text, 'OPENIVM_COMMIT', 'b'*40)
        self.assertEqual(result, text.replace('a'*40, 'b'*40))
        for invalid in ['main', 'abcd', 'b'*39]:
            with self.assertRaises(ValueError):
                study.replace_pin(text, 'OPENIVM_COMMIT', invalid)
        with self.assertRaises(ValueError):
            study.replace_pin(text.splitlines()[0], 'OPENIVM_COMMIT', 'b'*40)

    def test_both_engines_have_abba_order_and_native_validation_precedes_native_timings(self):
        pins = dict(spark_control='a'*40, spark_treatment='b'*40,
                    native_control='c'*40, native_treatment='d'*40)
        phases = study.phases(pins, 100)
        for engine in ['spark-openivm', 'duckdb-openivm']:
            measured = [p for p in phases if p[1] == engine and p[3] == 100]
            self.assertEqual(''.join(p[2] for p in measured), 'ABBA')
            self.assertEqual([p[4] for p in measured], [engine == 'spark-openivm']*4)
        native_checks = [i for i,p in enumerate(phases) if p[1] == 'duckdb-openivm' and p[4]]
        native_timing = [i for i,p in enumerate(phases) if p[1] == 'duckdb-openivm' and not p[4]]
        self.assertEqual(len(native_checks), 2)
        self.assertLess(max(native_checks), min(native_timing))


if __name__ == '__main__':
    unittest.main()
