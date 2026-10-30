"""Extended TPC-DS HTTP and CLI execution contract."""

import os
import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

APP = Path(__file__).resolve().parents[1]
os.environ['EXTENDED_TPCDS_WORKLOAD_DIR'] = str(APP.parents[1] / 'workloads' / 'extended-tpcds')
sys.path.insert(0,str(APP))
from services.extended_tpcds import CliConnection, build
from services.compiler_bench.engines import QueryFailed
from flask import Flask
from handlers.extended_tpcds import ExtendedTPCDSHandler


class ExtendedServiceTests(unittest.TestCase):
    def test_cli_accepts_multiple_result_sets_and_dates(self):
        with patch('services.extended_tpcds._DuckDBSession') as factory:
            session=factory.return_value
            session.execute.return_value='[{"x":null}]\n[{"version":"v1.5.4"}]\n'
            c=CliConnection('duckdb','state.duckdb')
            self.assertEqual(c.engine_version,'v1.5.4')
            session.execute.return_value='[{"first":"2002-12-31","n":5}]'
            self.assertEqual(c.execute('SELECT date,count').fetchone(),(date(2002,12,31),5))
            c.close()
            session.close.assert_called_once()

    def test_failed_extension_load_closes_worker(self):
        with patch('services.extended_tpcds._DuckDBSession') as factory:
            factory.return_value.execute.side_effect=QueryFailed('cannot load json')
            with self.assertRaisesRegex(QueryFailed,'cannot load'):
                CliConnection('duckdb','state.duckdb')
            factory.return_value.close.assert_called_once()

    def test_http_keeps_validation_outside_execution_request(self):
        app=Flask(__name__);ExtendedTPCDSHandler().register(app)
        with patch('services.extended_tpcds.run_batch',return_value={'status':'completed'}) as run, patch('services.extended_tpcds.validate_batch',return_value={'status':'failed','differences':{'m':2}}) as validate:
            client=app.test_client()
            self.assertEqual(client.post('/workloads/extended-tpcds/duckdb/batch/1').status_code,200)
            run.assert_called_once_with('duckdb',1,validate_results=False)
            response=client.post('/workloads/extended-tpcds/duckdb/validate/1')
            self.assertEqual(response.status_code,422)
            self.assertEqual(response.json['differences']['m'],2)
            validate.assert_called_once_with('duckdb',1)

    def test_create_rejection_has_explicit_table_fallback(self):
        c=Mock();c.session.last_sql_time_s=.01
        def execute(sql,*args):
            if sql.startswith('CREATE MATERIALIZED'):raise QueryFailed('unsupported window')
            if sql.startswith('PRAGMA refresh_status'):raise QueryFailed('view not registered')
            c.fetchone.return_value=(1,)
            return c
        c.execute.side_effect=execute
        with patch('services.extended_tpcds.pipeline.selected_models',return_value=['m']),patch('services.extended_tpcds.pipeline.render_model',return_value='SELECT 1 AS x'):
            result=build(c,{},'all',{'_engine':'duckdb-openivm'},'table')
        self.assertEqual(result['materializations']['m']['classification'],'unsupported_mv_table_fallback')
        self.assertEqual(result['materializations']['m']['create_message'],'unsupported window')
        self.assertTrue(any(call.args[0].startswith('CREATE TABLE m') for call in c.execute.call_args_list))

    def test_refresh_failure_is_not_hidden_by_table_fallback(self):
        c=Mock();c.execute.side_effect=QueryFailed('Binder Error: broken refresh')
        state={'_engine':'duckdb-openivm','m':{'kind':'materialized_view','classification':'incremental'}}
        with patch('services.extended_tpcds.pipeline.selected_models',return_value=['m']),patch('services.extended_tpcds.pipeline.render_model',return_value='SELECT 1'):
            with self.assertRaisesRegex(QueryFailed,'broken refresh'):
                build(c,{},'all',state,'table')
        self.assertEqual(c.execute.call_count,1)


if __name__=='__main__':unittest.main()
