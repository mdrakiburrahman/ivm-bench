"""CLI phase observation keeps complete output and enforces process timeouts."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

PATH = (Path(__file__).resolve().parents[1] / 'adapters/dbt-duckdb-openivm/'
        'dbt/adapters/openivm/cli_process.py')
spec = importlib.util.spec_from_file_location('openivm_cli_process', PATH)
process = importlib.util.module_from_spec(spec)
spec.loader.exec_module(process)


class OpenIVMCLIProcessTest(unittest.TestCase):
    def test_process_sampling_preserves_available_counters(self):
        with patch.object(process.Path, 'read_text', return_value='counter data'):
            sample = process.sample_process(12, 0.25, 'ready')
        self.assertEqual(sample['offset_s'], 0.25)
        self.assertEqual(sample['reason'], 'ready')
        self.assertEqual(sample['process_io'], 'counter data')
        with patch.object(process.Path, 'read_text', side_effect=FileNotFoundError):
            sample = process.sample_process(12, 0.25, 'gone')
        self.assertIsNone(sample['process_io'])

    def test_large_input_and_output_preserve_phase_markers(self):
        payload = 'x' * (1024 * 1024)
        script = (
            "import sys\n"
            "print('READY', flush=True)\n"
            "data=sys.stdin.read()\n"
            "print('STARTED', flush=True)\n"
            "sys.stdout.write(data); sys.stdout.flush()\n"
            "print('\\nFINISHED', flush=True)\n"
        )
        result, elapsed, offsets = process.run_profiled_cli(
            [sys.executable, '-c', script], payload,
            {'ready': 'READY', 'started': 'STARTED', 'finished': 'FINISHED'}, timeout=10,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, f'READY\nSTARTED\n{payload}\nFINISHED\n')
        self.assertLessEqual(offsets['spawn_return_offset_s'], offsets['ready'])
        self.assertLessEqual(offsets['ready'], offsets['started'])
        self.assertLessEqual(offsets['started'], offsets['finished'])
        self.assertLessEqual(offsets['finished'], elapsed)
        self.assertTrue(offsets['process_samples'])

    def test_setup_failure_retains_error_and_missing_sql_markers(self):
        result, _, offsets = process.run_profiled_cli(
            [sys.executable, '-c', "import sys; print('database is locked',file=sys.stderr); sys.exit(1)"],
            '', {'started': 'STARTED'}, timeout=10,
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn('database is locked', result.stdout)
        self.assertNotIn('started', offsets)

    def test_expired_deadline_reaps_the_child(self):
        children = []
        popen = subprocess.Popen

        def spawn(*args, **kwargs):
            child = popen(*args, **kwargs)
            children.append(child)
            return child

        with patch.object(process.subprocess, 'Popen', side_effect=spawn):
            with self.assertRaises(subprocess.TimeoutExpired):
                process.run_profiled_cli(
                    [sys.executable, '-c', 'import signal; signal.pause()'],
                    '', {'started': 'STARTED'}, timeout=0,
                )
        self.assertIsNotNone(children[0].returncode)


if __name__ == '__main__':
    unittest.main()
