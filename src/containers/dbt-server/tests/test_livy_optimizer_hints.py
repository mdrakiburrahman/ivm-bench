"""Exercise the exact dependency patch applied by the dbt-server image."""
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from dbt.adapters.fabricspark import livysession


class LivyOptimizerHintsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / 'livysession.py'
            shutil.copyfile(livysession.__file__, source)
            patch = Path(__file__).parents[1] / 'patches/livy-preserve-optimizer-hints.patch'
            subprocess.run(['git', '-C', folder, 'apply', str(patch)], check=True, capture_output=True)
            spec = importlib.util.spec_from_file_location('patched_livy_hint_test', source)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            cls.transport = module.LivyCursor._getLivySQL

    def test_optimizer_hints_survive_after_dbt_comments(self):
        sql = '/* dbt metadata */ SELECT /*+ MERGE(m) */ a.k FROM a JOIN m ON a.k=m.k'
        self.assertEqual(self.transport(sql), 'SELECT /*+ MERGE(m) */ a.k FROM a JOIN m ON a.k=m.k')

    def test_multiline_hints_survive(self):
        sql = 'SELECT /*+\n MERGE(m)\n */ a.k FROM a JOIN m ON a.k=m.k'
        self.assertEqual(self.transport(sql), sql)

    def test_ordinary_comments_and_unhinted_queries_keep_existing_behavior(self):
        self.assertEqual(self.transport('/* metadata */ SELECT 1 /* trailing */'), 'SELECT 1')
        self.assertEqual(self.transport('SELECT 1'), 'SELECT 1')
