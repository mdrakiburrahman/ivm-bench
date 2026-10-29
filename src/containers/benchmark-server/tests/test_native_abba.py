"""Require balanced native runs and complete command/observer coverage."""
import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]
spec = importlib.util.spec_from_file_location('native_abba', ROOT/'tools/scripts/benchmarks/native_abba.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class NativeABBATest(unittest.TestCase):
    def test_merged_pr_rerun_measures_the_new_head_once(self):
        path = ROOT/'src/containers/benchmark-server/experiments/native-pr119-merged-sf100-sf10.json'
        study = json.loads(path.read_text())['_attribution']
        phases = runner.phases(study, 100)
        self.assertEqual(len(phases), 2)
        self.assertEqual([(p[2], p[3], p[4]) for p in phases], [('B', 10, True), ('B', 100, False)])
        self.assertEqual({p[5] for p in phases}, {'c0669521c4777b3e951869ef77052f4287c68b6a'})

    def test_exact_sources_and_balanced_fresh_repetitions(self):
        study = {'native_control': 'a'*40, 'native_treatment': 'b'*40}
        phases = runner.phases(study, 100)
        self.assertEqual([p[2] for p in phases], list('ABABBA'))
        self.assertEqual([p[3] for p in phases], [10, 10, 100, 100, 100, 100])
        self.assertEqual([p[4] for p in phases], [True, True, False, False, False, False])
        self.assertTrue(all(p[1] == 'duckdb-openivm' and p[5] == study[
            'native_control' if p[2] == 'A' else 'native_treatment'] for p in phases))
        docker = 'ARG OPENIVM_COMMIT='+'c'*40+'\nARG OPENIVM_COMMIT='+'c'*40+'\n'
        self.assertEqual(runner.replace_pin(docker, 'OPENIVM_COMMIT', 'a'*40).count('a'*40), 2)
        with self.assertRaises(ValueError):
            runner.replace_pin(docker, 'OPENIVM_COMMIT', 'main')

    def test_missing_process_observer_or_command_fails_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/'oat').mkdir()
            directory = root/'dbt-server'
            directory.mkdir()
            (directory/'benchmark-results.json').write_text(json.dumps(
                {'status': 'completed', 'repetition_count': 1}))
            with (root/'oat/results.csv').open('w') as f:
                writer = csv.DictWriter(f, fieldnames=['batch_num', 'batch_status', 'batch_error',
                    'engine', 'scale_factor', 'openivm_validate'])
                writer.writeheader()
                for b in (1, 2, 3):
                    writer.writerow(dict(batch_num=b, batch_status='completed', batch_error='',
                        engine='duckdb-openivm', scale_factor=100, openivm_validate='false'))
            rows = []
            for batch in (1, 2, 3):
                for i in range(49):
                    rows.append(dict(timestamp_s=f'{batch}.{i}', sql=f"PRAGMA refresh_options('v{i}')",
                        attempt='0', returncode='0', ready_offset_s='0.1', sql_started_offset_s='0.2',
                        sql_finished_offset_s='0.3', exit_offset_s='0.4',
                        process_samples_json=json.dumps([{'process_io': 'write_bytes: 0'}])))
                path = directory/f'openivm-cli-timings-batch{batch}.csv'
                with path.open('w') as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
            self.assertEqual(len(runner.verify_results(root, 100, 'duckdb-openivm', False)), 3)
            path = directory/'openivm-cli-timings-batch3.csv'
            saved = path.read_text()
            with path.open() as f:
                data = list(csv.DictReader(f))
            data[-1]['process_samples_json'] = '[]'
            with path.open('w') as f:
                writer = csv.DictWriter(f, fieldnames=list(data[0])); writer.writeheader(); writer.writerows(data)
            with self.assertRaisesRegex(RuntimeError, 'I/O samples'):
                runner.verify_results(root, 100, 'duckdb-openivm', False)
            path.write_text('\n'.join(saved.splitlines()[:-1])+'\n')
            with self.assertRaisesRegex(RuntimeError, 'timing coverage'):
                runner.verify_results(root, 100, 'duckdb-openivm', False)


if __name__ == '__main__':
    unittest.main()
