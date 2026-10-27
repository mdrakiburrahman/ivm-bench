"""Native SQL export retains full multiline statements and rejects partial captures."""

import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

PATH = Path(__file__).resolve().parents[1]/'services/openivm_profile.py'
spec = importlib.util.spec_from_file_location('native_profile_capture', PATH)
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class NativeSQLCaptureTest(unittest.TestCase):
    def test_wrapper_preserves_arguments_input_and_failure_exit_status(self):
        source = Path(__file__).resolve().parents[2]/'duckdb-openivm/capture_sql.py'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            wrapper = root/'duckdb'
            shutil.copyfile(source, wrapper)
            wrapper.chmod(0o755)
            binary = root/'duckdb.real'
            binary.write_text(f'#!{sys.executable}\n' +
                              'import json,sys\n'
                              'print(json.dumps(dict(argv=sys.argv[1:],stdin=sys.stdin.read())))\n'
                              'sys.exit(7 if "--fail" in sys.argv else 0)\n')
            binary.chmod(0o755)
            env = dict(os.environ, OPENIVM_QUERY_LOG='1', DUCKDB_OPENIVM_WORK_DIR=str(root/'work'))
            for args,sql in [(['--fail'], 'SELECT 1;\n'), (['-c', 'SELECT 2;'], '')]:
                result = subprocess.run([str(wrapper), *args], input=sql, text=True,
                                        capture_output=True, env=env, timeout=10)
                self.assertEqual(result.returncode, 7 if '--fail' in args else 0)
                self.assertEqual(result.stderr, '')
                actual = json.loads(result.stdout)
                if sql:
                    self.assertEqual(actual['argv'], args)
                    self.assertTrue(actual['stdin'].endswith(sql))
                else:
                    self.assertEqual(actual['argv'][0], '-c')
                    self.assertTrue(actual['argv'][1].endswith(args[1]))
            requests = [json.loads(p.read_text()) for p in (root/'work/_sql_trace').glob('*/request.json')]
            self.assertEqual(len(requests), 2)
            self.assertEqual({r['stdin_sql'] or r['command_sql'][0] for r in requests},
                             {'SELECT 1;\n', 'SELECT 2;'})
            env['OPENIVM_QUERY_LOG'] = '0'
            result = subprocess.run([str(wrapper)], input='SELECT 3;', text=True,
                                    capture_output=True, env=env, timeout=10)
            self.assertEqual(json.loads(result.stdout), dict(argv=[], stdin='SELECT 3;'))
            self.assertEqual(len(list((root/'work/_sql_trace').glob('*/request.json'))), 2)

    def test_round_trip_keeps_large_multiline_sql_and_distinct_invocations(self):
        sql = "SELECT '" + ('x,\"\n' * 100000) + "' AS payload;"
        fields = ['context_id', 'scope', 'connection_id', 'transaction_id', 'query_id',
                  'thread_id', 'timestamp', 'type', 'log_level', 'message']
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for token in ['a', 'b']:
                directory = root/token
                directory.mkdir()
                (directory/'request.json').write_text(json.dumps(dict(
                    invocation_id=token, created_at=1, argv=['test.duckdb'],
                    stdin_sql=sql, command_sql=[])))
                with (directory/'queries.csv').open('w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fields)
                    writer.writeheader()
                    writer.writerow(dict(type='QueryLog', message=sql))
            # CSV's default 128-KiB field cap is lower than real refresh programs.
            result = profile.export_query_log(root)
            rows = list(csv.DictReader(io.StringIO(result['queries_csv'])))
            requests = list(csv.DictReader(io.StringIO(result['requests_csv'])))
            self.assertEqual(result['invocation_count'], 2)
            self.assertEqual([r['submitted_sql'] for r in requests], [sql, sql])
            self.assertEqual([r['message'] for r in rows if r['type']=='QueryLog'], [sql, sql])
            self.assertEqual({r['invocation_id'] for r in rows}, {'a', 'b'})

    def test_missing_trace_and_empty_capture_fail_instead_of_claiming_equality(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaisesRegex(RuntimeError, 'empty'):
                profile.export_query_log(root)
            directory = root/'a'
            directory.mkdir()
            (directory/'request.json').write_text(json.dumps(dict(
                invocation_id='a', created_at=1, argv=[], stdin_sql='SELECT 1', command_sql=[])))
            with self.assertRaises(FileNotFoundError):
                profile.export_query_log(root)


if __name__ == '__main__':
    unittest.main()
