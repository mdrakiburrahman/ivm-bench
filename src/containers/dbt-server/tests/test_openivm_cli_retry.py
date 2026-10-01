"""A failed CLI command is not proof that its writes rolled back."""
import csv
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

MODULE_PATH = (Path(__file__).resolve().parents[1] / 'adapters/dbt-duckdb-openivm/'
               'dbt/adapters/openivm/connections.py')
spec = importlib.util.spec_from_file_location('openivm_cli_connections', MODULE_PATH)
connections = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(connections)
    HAS_ADAPTER = True
except ModuleNotFoundError as error:
    if not error.name.startswith(('dbt', 'agate')):
        raise
    HAS_ADAPTER = False


@unittest.skipUnless(HAS_ADAPTER, "requires dbt-server adapter dependencies")
class OpenIVMCLIRetryTest(unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(tempfile.TemporaryDirectory())
        self.enterContext(patch.object(connections, 'TEMP_DIR', directory))
        self.enterContext(patch.object(connections, 'MAX_RETRIES', 2))
        self.enterContext(patch.object(connections, 'PROFILE_REFRESH', False))
        self.sleep = self.enterContext(patch.object(connections.time, 'sleep'))

    @staticmethod
    def output(program, body='', started=True, finished=True, returncode=0):
        markers = [line.removeprefix('.print ') for line in program.splitlines()
                   if line.startswith('.print OPENIVM_SQL_')]
        text = (markers[0] + '\n' if started else '') + body
        text += markers[1] + '\n' if finished else ''
        return subprocess.CompletedProcess([], returncode, text)

    def test_snapshot_publication_setting_is_opt_in(self):
        def execute(*args, **kwargs):
            return self.output(kwargs['input'])
        for enabled in (False, True):
            with self.subTest(enabled=enabled), \
                    patch.object(connections, 'SNAPSHOT_PUBLICATION', enabled), \
                    patch.object(connections.subprocess, 'run', side_effect=execute) as run:
                connections._run_cli('SELECT 1')
                self.assertEqual('SET openivm_snapshot_publication=true;' in run.call_args.kwargs['input'], enabled)

    def test_setup_lock_retries_before_sql_and_preserves_output(self):
        def execute(*args, **kwargs):
            if run.call_count == 1:
                return self.output(kwargs['input'], 'database is locked\n', False, False, 1)
            return self.output(kwargs['input'], 'a,b\n1,2\n')
        with patch.object(connections.subprocess, 'run', side_effect=execute) as run:
            self.assertEqual(connections._run_cli('SELECT 1, 2'), 'a,b\n1,2\n')
        self.assertEqual(run.call_count, 2)
        self.sleep.assert_called_once()

    def test_profile_separates_setup_sql_and_process_overhead_without_changing_results(self):
        def execute(*args, **kwargs):
            result = self.output(kwargs['input'],
                                 'a,b\n1,2\nRun Time (s): real 1.000 user 0.5 sys 0.1\n'
                                 'Run Time (s): real 0.500 user 0.2 sys 0.1\n')
            result.stdout = 'Run Time (s): real 0.250 user 0.1 sys 0.0\n' + result.stdout
            return result
        sql = 'SELECT "a,b", 2'
        with patch.object(connections, 'PROFILE_REFRESH', True), \
                patch.object(connections.time, 'monotonic', side_effect=[10.0, 13.0]), \
                patch.object(connections.subprocess, 'run', side_effect=execute):
            self.assertEqual(connections._run_cli(sql), 'a,b\n1,2\n')
        with open(Path(connections.TEMP_DIR) / 'cli-timings.csv', newline='') as trace:
            rows = list(csv.reader(trace))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], sql)
        self.assertEqual([float(v) for v in rows[0][4:]], [3.0, 0.25, 1.5, 1.25])

    def test_profile_write_failure_does_not_fail_or_replay_completed_sql(self):
        def execute(*args, **kwargs):
            return self.output(kwargs['input'], '42\nRun Time (s): real 0.001 user 0 sys 0\n')
        with patch.object(connections, 'PROFILE_REFRESH', True), \
                patch('builtins.open', side_effect=OSError('trace unavailable')), \
                patch.object(connections.subprocess, 'run', side_effect=execute) as run, \
                self.assertLogs(connections.logger, level='WARNING'):
            self.assertEqual(connections._run_cli('SELECT 42'), '42\n')
        self.assertEqual(run.call_count, 1)

    def test_execution_lock_is_not_replayed_and_original_error_survives(self):
        def execute(*args, **kwargs):
            return self.output(kwargs['input'], 'CREATE succeeded\ndatabase is locked: original detail\n',
                               finished=False, returncode=1)
        with patch.object(connections.subprocess, 'run', side_effect=execute) as run:
            with self.assertRaisesRegex(connections.DbtDatabaseError, 'original detail') as error:
                connections._run_cli('CREATE TABLE t AS SELECT 1; SELECT 2')
        self.assertIn('not replaying', str(error.exception))
        self.assertEqual(run.call_count, 1)
        self.sleep.assert_not_called()

    def test_teardown_failure_is_not_silently_accepted_or_replayed(self):
        def execute(*args, **kwargs):
            return self.output(kwargs['input'], 'database is locked\n', returncode=1)
        with patch.object(connections.subprocess, 'run', side_effect=execute) as run:
            with self.assertRaisesRegex(connections.DbtDatabaseError, 'after SQL completion'):
                connections._run_cli('INSERT INTO t VALUES (1)')
        self.assertEqual(run.call_count, 1)
        self.sleep.assert_not_called()

    def test_setup_retries_are_bounded_and_log_original_errors(self):
        with patch.object(connections.subprocess, 'run', return_value=
                          subprocess.CompletedProcess([], 1, 'database is locked: setup detail')) as run:
            with self.assertLogs(connections.logger, level='WARNING') as logs:
                with self.assertRaisesRegex(connections.DbtDatabaseError, 'attempt 3/3'):
                    connections._run_cli('SELECT 1')
        self.assertEqual(run.call_count, 3)
        self.assertEqual(self.sleep.call_count, 2)
        self.assertTrue(all('setup detail' in line for line in logs.output))

    def test_signal_exit_and_missing_completion_are_not_success_or_retry(self):
        for code in (-9, 0):
            with self.subTest(code=code), patch.object(connections.subprocess, 'run', return_value=
                    subprocess.CompletedProcess([], code, 'database is locked')) as run:
                with self.assertRaises(connections.DbtDatabaseError):
                    connections._run_cli('SELECT 1')
                self.assertEqual(run.call_count, 1)
        self.sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
