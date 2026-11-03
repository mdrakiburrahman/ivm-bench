import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.oat_runner import _archive_failure_forensics


class WorkloadFailureForensicsTest(unittest.TestCase):
    def test_partial_sql_timeline_survives_cleanup_without_copying_delta_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'mount/results/1/databricks-enzyme/extended-tpcds'
            source.mkdir(parents=True)
            (source / 'sql-batch1.json').write_text('[{"status":"completed"}]')
            (source / 'sql-checkpoint.json').write_text('{"dirty":true}')
            (source / 'sources').mkdir()
            (source / 'sources/part.parquet').write_bytes(b'large data')
            (source / 'unrelated.parquet').write_bytes(b'large data')
            archive = root / 'archive'
            _archive_failure_forensics(repo_dir=str(root), mount=str(root/'mount'), sf='1',
                                       engines=['databricks-enzyme', 'absent'],
                                       archive_dir=str(archive), emit=lambda _: None)
            result = archive / 'databricks-enzyme-extended-tpcds'
            self.assertEqual(sorted(p.name for p in result.iterdir()),
                             ['sql-batch1.json', 'sql-checkpoint.json'])
            self.assertEqual((result / 'sql-batch1.json').read_text(), '[{"status":"completed"}]')
