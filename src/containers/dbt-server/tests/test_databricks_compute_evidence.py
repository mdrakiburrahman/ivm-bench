import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).parents[1]))
from services import databricks_enzyme_sources as sources
from services.databricks_enzyme_pipeline_events import _pipeline_compute


class DatabricksComputeEvidenceTests(unittest.TestCase):
    def test_warehouse_settings_capture_limits_and_exclude_secrets(self):
        raw=dict(id='w',enable_serverless_compute=True,enable_photon=True,cluster_size='Small',
                 min_num_clusters=1,max_num_clusters=1,channel={'name':'CHANNEL_NAME_CURRENT'},
                 jdbc_url='secret',configuration={'token':'secret'})
        ws=Mock();ws.warehouses.get.return_value.as_dict.return_value=raw
        with patch.object(sources,'_workspace_client',return_value=ws), \
             patch.dict(os.environ,{'DATABRICKS_SQL_HTTP_PATH':'/sql/1.0/warehouses/w'}):
            settings=sources.warehouse_settings()
        ws.warehouses.get.assert_called_once_with(id='w')
        self.assertEqual(settings['cluster_size'],'Small')
        self.assertEqual(settings['max_num_clusters'],1)
        self.assertTrue(settings['enable_serverless_compute'])
        self.assertNotIn('secret',json.dumps(settings))

    def test_pipeline_settings_do_not_claim_a_fixed_serverless_allocation(self):
        ws=Mock()
        ws.pipelines.get.return_value.spec.as_dict.return_value={
            'serverless':True,'photon':True,'channel':'CURRENT','configuration':{'key':'secret'}}
        settings=_pipeline_compute(ws,'p')
        ws.pipelines.get.assert_called_once_with(pipeline_id='p')
        self.assertTrue(settings['serverless'])
        self.assertEqual(settings['fixed_worker_allocation'],'not_exposed_for_serverless')
        self.assertNotIn('secret',json.dumps(settings))
        ws.pipelines.get.side_effect=PermissionError('secret')
        self.assertEqual(_pipeline_compute(ws,'p'),{'status':'unavailable','error_type':'PermissionError'})
