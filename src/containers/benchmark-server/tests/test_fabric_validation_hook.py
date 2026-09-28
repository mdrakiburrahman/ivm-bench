import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models.result import EngineResult
from services.engine_runner import EngineRunner, OpenIvmValidationError


class FabricValidationHookTest(unittest.TestCase):
    def runner(self, name='fabric-openivm-jvm-35'):
        runner=EngineRunner.__new__(EngineRunner)
        runner._engine=SimpleNamespace(name=name)
        runner._result=EngineResult(name)
        runner._emit=MagicMock()
        runner._persist_batch_result=MagicMock()
        runner._run_fabric=MagicMock(return_value='run')
        runner._save_openivm_ops_chart=MagicMock()
        runner._capture_storage_metrics=MagicMock()
        runner._ensure_cpu_measurement_status=MagicMock()
        return runner

    def test_fabric_validation_runs_after_timer_and_failure_marks_batch_failed(self):
        runner=self.runner()
        def validate(run_id,batch_num):
            self.assertEqual(runner._result.batches[0].duration_s,10)
            self.assertEqual((run_id,batch_num),('run',1))
            raise OpenIvmValidationError('mismatch')
        runner._validate_spark_openivm=MagicMock(side_effect=validate)
        with patch.dict('os.environ',{'OPENIVM_VALIDATE':'1'}), patch('services.engine_runner.time.time',side_effect=[0,10,10]):
            with self.assertRaises(OpenIvmValidationError):
                runner._run_batch(1)
        self.assertEqual(runner._result.batches[0].status,'failed')

    def test_disabled_validation_and_non_ivm_counterpart_do_not_validate(self):
        for name,enabled in [('fabric-openivm-jvm-35','0'),('fabric-jvm-35','1')]:
            runner=self.runner(name)
            runner._validate_spark_openivm=MagicMock()
            with patch.dict('os.environ',{'OPENIVM_VALIDATE':enabled}), patch('services.engine_runner.time.time',side_effect=[0,10,10]):
                runner._run_batch(1)
            runner._validate_spark_openivm.assert_not_called()

    def test_endpoint_and_artifact_use_actual_engine_and_reject_failure(self):
        runner=self.runner()
        with tempfile.TemporaryDirectory() as directory:
            runner._config=SimpleNamespace(repo_dir=directory,scale_factor=100)
            runner._dbt_url='http://dbt-server'
            response=MagicMock(status_code=500)
            response.json.return_value={'status':'failed','failures':[{'name':'x','diff_count':1}]}
            with patch('services.engine_runner.requests.post',return_value=response) as post:
                with self.assertRaises(OpenIvmValidationError):
                    runner._validate_spark_openivm('run',2)
            self.assertEqual(post.call_args.args[0],'http://dbt-server/validate/fabric-openivm-jvm-35/run')
            artifact=Path(directory)/'mount/results/100/dbt-server/validation-fabric-openivm-jvm-35-batch2.json'
            self.assertEqual(json.loads(artifact.read_text())['status'],'failed')


if __name__ == '__main__':
    unittest.main()
