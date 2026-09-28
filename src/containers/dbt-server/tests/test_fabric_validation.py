import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock, mock_open, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.fabric_validation import FabricValidationClient
from services import spark_openivm_validation as validator


class FabricValidationTest(unittest.TestCase):
    def test_attach_only_and_output_normalization(self):
        credentials = MagicMock(return_value=types.SimpleNamespace(session_id_file='/session'))
        session = MagicMock(session_id='saved', is_new_session_required=False)
        cursor = MagicMock()
        cursor.fetchall.return_value = [[3, 42]]
        modules = {
            'dbt.adapters.fabricspark.credentials': types.SimpleNamespace(FabricSparkCredentials=credentials),
            'dbt.adapters.fabricspark.livysession': types.SimpleNamespace(LivySession=MagicMock(return_value=session), LivyCursor=MagicMock(return_value=cursor)),
        }
        with patch.dict(sys.modules, modules), patch.dict('os.environ', {'FABRIC_WORKSPACE_ID':'workspace'}), \
             patch('pathlib.Path.read_text', side_effect=[json.dumps({'lakehouse_name':'compute', 'lakehouse_id':'lake'}), 'saved']):
            with FabricValidationClient() as client:
                self.assertEqual(client.execute('SELECT 3, 42')['output']['data']['application/json']['data'], [[3,42]])
                session.try_reuse_session.assert_called_once_with('saved')
                session.is_new_session_required = True
                with self.assertRaisesRegex(RuntimeError, 'lost'):
                    client.execute('SELECT 1')
        session.close.assert_not_called()
        cursor.execute.assert_called_once_with('SELECT 3, 42')
        cursor.close.assert_called_once()

    def test_missing_live_session_never_creates_replacement(self):
        session = MagicMock()
        session.try_reuse_session.return_value = False
        modules = {
            'dbt.adapters.fabricspark.credentials': types.SimpleNamespace(FabricSparkCredentials=MagicMock(return_value=types.SimpleNamespace(session_id_file='/session'))),
            'dbt.adapters.fabricspark.livysession': types.SimpleNamespace(LivySession=MagicMock(return_value=session)),
        }
        with patch.dict(sys.modules, modules), patch.dict('os.environ', {'FABRIC_WORKSPACE_ID':'workspace'}), \
             patch('pathlib.Path.read_text', side_effect=[json.dumps({'lakehouse_name':'compute', 'lakehouse_id':'lake'}), 'saved']):
            with self.assertRaisesRegex(RuntimeError, 'must not create'):
                with FabricValidationClient():
                    self.fail('dead session admitted')

    def fixture(self, nodes=None, engine='fabric-openivm-jvm-35', status='completed'):
        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = {'engine':engine,'status':status}
        connection.execute.return_value.fetchall.return_value = nodes if nodes is not None else [
            {'unique_id':'model.x', 'name':'x', 'resource_type':'model', 'status':'success','compiled_sql':'SELECT 1;'}]
        return connection

    def test_reuses_comparison_with_fabric_schema_and_own_session(self):
        client = MagicMock()
        client.lakehouse = 'compute'
        factory = MagicMock()
        factory.return_value.__enter__.return_value = client
        with patch.object(validator,'get_db',return_value=self.fixture()), \
             patch('builtins.open',mock_open(read_data=json.dumps({'nodes':{'model.x':{'schema':'compute'}}}))), \
             patch('services.fabric_validation.FabricValidationClient',factory), \
             patch.object(validator,'_validate_one',return_value={'status':'pass'}) as compare, \
             patch('services.dbt_compiler.get_compiled_models') as compile_models:
            result = validator.validate_run('run',engine='fabric-openivm-jvm-35')
        self.assertEqual(result['models_checked'],1)
        self.assertEqual(result['status'],'passed')
        self.assertEqual(compare.call_args.kwargs['compiled_sql'],'SELECT 1')
        self.assertEqual(compare.call_args.kwargs['schema'],'compute')
        client.execute.assert_called_once_with('USE `compute`')
        compile_models.assert_not_called()

    def test_zero_models_failed_run_and_wrong_engine_are_not_passes(self):
        for connection in (self.fixture(nodes=[]), self.fixture(status='failed'), self.fixture(engine='fabric-jvm-35')):
            with patch.object(validator,'get_db',return_value=connection), \
                 patch('builtins.open',mock_open(read_data='{"nodes":{}}')), \
                 patch('services.fabric_validation.FabricValidationClient') as factory:
                with self.assertRaises(ValueError):
                    validator.validate_run('run',engine='fabric-openivm-jvm-35')
                factory.assert_not_called()

    def test_missing_sql_or_schema_and_failed_models_reject_partial_validation(self):
        for update in ({'compiled_sql':''},{'status':'error'},{}):
            node={'unique_id':'model.x','name':'x','resource_type':'model','status':'success','compiled_sql':'SELECT 1'}
            node.update(update)
            with patch.object(validator,'get_db',return_value=self.fixture(nodes=[node])), \
                 patch('builtins.open',mock_open(read_data='{"nodes":{}}')):
                with self.assertRaises(ValueError):
                    validator.validate_run('run',engine='fabric-openivm-jvm-35')

    def test_catalog_selection_failure_is_fatal(self):
        client=MagicMock(lakehouse='compute')
        client.execute.side_effect=RuntimeError('wrong lakehouse')
        with patch.object(validator,'get_db',return_value=self.fixture()), \
             patch('builtins.open',mock_open(read_data='{"nodes":{"model.x":{"schema":"compute"}}}')), \
             patch('services.fabric_validation.FabricValidationClient') as factory:
            factory.return_value.__enter__.return_value=client
            with self.assertRaisesRegex(RuntimeError,'wrong lakehouse'):
                validator.validate_run('run',engine='fabric-openivm-jvm-35')


if __name__ == '__main__':
    unittest.main()
