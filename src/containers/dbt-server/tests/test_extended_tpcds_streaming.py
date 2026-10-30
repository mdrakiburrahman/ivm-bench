import importlib.util
import json
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch

APP=Path(__file__).parents[1]
sys.path.insert(0,str(APP))
os.environ['EXTENDED_TPCDS_WORKLOAD_DIR']=str(APP.parents[1]/'workloads/extended-tpcds')
spec=importlib.util.spec_from_file_location('extended_tpcds_streaming_tested',APP/'services/extended_tpcds_streaming.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ExtendedTPCDSStreamingTest(unittest.TestCase):
    def image(self,root,name,ids,values):
        path=root/(name+'.parquet')
        module.pq.write_table(module.pa.table({'_source_row_id':ids,'value':values}),path)
        return {'path':path.name,'rows':len(ids),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

    def test_real_delta_replay_preserves_identity_and_retracts_updates_and_deletes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);work=root/'work'
            initial=self.image(root,'initial',['a','b'],[1,2])
            before_update=self.image(root,'update_before',['a'],[1]);after_update=self.image(root,'update_after',['a'],[3])
            before_delete=self.image(root,'delete_before',['b'],[2]);insert=self.image(root,'insert',['c'],[4])
            manifest={'schemas':{'events':{'_source_row_id':'VARCHAR','value':'INTEGER'}},'initial':{'events':initial},'batches':[{'operations':[
                {'table':'events','kind':'update','before':before_update,'after':after_update},
                {'table':'events','kind':'delete','before':before_delete,'after':None},
                {'table':'events','kind':'insert','before':None,'after':insert}]}]}
            module.replay(manifest,root,work,1)
            table_id=module.DeltaTable(str(work/'sources/events')).metadata().id
            module.replay(manifest,root,work,2)
            table=module.DeltaTable(str(work/'sources/events'))
            self.assertEqual(table.metadata().id,table_id)
            self.assertEqual(sorted((r['_source_row_id'],r['value']) for r in table.to_pyarrow_table().to_pylist()),[('a',3),('c',4)])
            with self.assertRaisesRegex(ValueError,'before-image mismatch'):
                module.replay(manifest,root,work,2)

    def test_checksum_rejection_happens_before_source_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);image=self.image(root,'initial',['a'],[1]);image['sha256']='wrong'
            with patch.object(module,'write_deltalake') as write:
                with self.assertRaisesRegex(ValueError,'checksum'):
                    module.replay({'initial':{'events':image}},root,root/'work',1)
                write.assert_not_called()

    def test_dbsp_error_keeps_original_server_failure(self):
        response=Mock(ok=False,status_code=500,text='System.OutOfMemoryException: aggregate resize')
        with patch.object(module.requests,'post',return_value=response):
            with self.assertRaisesRegex(RuntimeError,'OutOfMemoryException'):
                module._dbsp_request('/wait')
        response.json.assert_not_called()

    def test_feldera_wait_uses_pre_resume_progress_for_fast_batches(self):
        from services import feldera_client
        stats={'global_metrics':{'total_initiated_steps':2,'total_completed_steps':2,'pipeline_complete':True},'outputs':[]}
        with patch.object(feldera_client,'get_stats',return_value=stats) as get_stats:
            success,_,_=feldera_client.poll_output_completion(baseline_steps=1)
        self.assertTrue(success)
        self.assertEqual(get_stats.call_count,1)

    def test_feldera_translation_preserves_null_on_error_and_global_regex(self):
        sql="SELECT TRY_CAST(x AS INTEGER), TRY_STRPTIME(d,'%Y%m%d'), json_extract_string(j,'$.CustomerPath'), regexp_replace(x,'a','b','g')"
        actual=module.render(sql,'feldera')
        self.assertIn('SAFE_CAST(x AS INT)',actual)
        self.assertIn("PARSE_DATE('%Y%m%d', d)",actual)
        self.assertIn("PARSE_JSON(j)['CustomerPath']",actual)
        self.assertIn("REGEXP_REPLACE(x, 'a', 'b')",actual)
        self.assertNotIn('TRY_STRPTIME',actual)

    def test_all_models_are_outputs_and_compile_failure_cannot_drop_models(self):
        manifest={'schemas':{'events':{'_source_row_id':'VARCHAR'}},'source_configuration':{'report_date':'2002-12-31','history_months':12},'_query_cohort':'all'}
        spec=module.program(manifest,Path('/data/processed/dbspnet/extended-tpcds'),'dbspnet')
        self.assertEqual(len(spec['outputs']),96)
        self.assertEqual(len(spec['program']),97)
        self.assertTrue(all(o['mode']=='truncate' for o in spec['outputs']))
        # TRY_CAST remains explicit: unsupported syntax must fail at compile,
        # not acquire throwing-cast semantics from a dialect conversion.
        self.assertIn('TRY_CAST','\n'.join(spec['program']))


if __name__=='__main__':unittest.main()
