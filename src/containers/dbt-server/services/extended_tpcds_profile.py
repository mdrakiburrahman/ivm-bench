"""Batch-scoped telemetry collected on the workload's own sessions, before validation."""
import logging
import os
import time
from uuid import uuid4
from sql_catalog import load_sql

from services import spark_openivm_profile as steps

STEP_ENGINES = ('duckdb-openivm', 'spark-openivm', 'fabric-openivm-jvm-35')


class BatchProfile:
    def __init__(self, engine, batch_num, models, connection=None):
        self.engine, self.batch_num, self.models = engine, batch_num, set(models)
        self.connection = connection
        self.enabled = os.environ.get('OPENIVM_PROFILE_REFRESH', '0') == '1'
        self.started_at_ms = time.time() * 1000
        self.capture_id = str(uuid4())
        self.previous_ids = set()
        self.error = None
        if self.enabled and engine in STEP_ENGINES:
            try:
                connection.execute('SET '+('openivm_profile_refresh=true' if engine == 'duckdb-openivm'
                                           else 'spark.openivm.profile.refresh=true'))
                if batch_num > 1:
                    self.previous_ids = {str(r['refresh_id']) for r in self._rows()}
            except Exception as error:
                self.error = str(error)

    def _rows(self):
        if self.engine == 'duckdb-openivm':
            return self.connection.execute(load_sql(
                'duckdb-openivm','inline','dbt_server/services_openivm_profile/profile_rows',
                exported_columns=f'{self.batch_num} AS exported_after_batch')).rows
        rows = self.connection.execute('SHOW OPENIVM REFRESH PROFILE')
        if len(rows) in (1000, 100000) or len(rows) > 100000:
            raise RuntimeError('refresh profile reached a Livy row limit; refusing a truncated export')
        if any(len(row) != len(steps._PROFILE_COLS) for row in rows):
            raise RuntimeError('unexpected refresh profile schema')
        return [dict(zip(steps._PROFILE_COLS, row)) for row in rows]

    def finish(self, model_commands, *, statements=(), incremental_models=()):
        payload = {'engine':self.engine, 'batch_num':self.batch_num, 'enabled':self.enabled,
                   'capture_id':self.capture_id,
                   'status':'disabled', 'scope':'batch', 'collection_outside_batch_timer':True,
                   'model_command_s':model_commands}
        if not self.enabled:
            return payload
        try:
            if self.error:
                raise RuntimeError(self.error)
            if self.engine in STEP_ENGINES:
                rows = [r for r in self._rows() if str(r['refresh_id']) not in self.previous_ids
                        and self._workload_view(r['view_name'])]
                if not rows:
                    raise RuntimeError('profiling enabled but no fresh workload refresh steps were exported')
                payload.update(steps.profile_payload(rows, self.capture_id, self.batch_num), kind='refresh_steps')
                covered = {str(r['view_name']).replace('`','').split('.')[-1] for r in rows}
                payload['models_without_steps'] = sorted(self.models-covered)
                if payload['models_without_steps']:
                    payload['status'] = 'partial'
                # CREATE eligibility cannot prevent runtime source-replacement
                # fallbacks. Keep the steps and report the strategy actually run.
                full = sorted({str(r['view_name']).replace('`','').split('.')[-1] for r in rows
                               if r['step_name'] == 'execute_full_refresh_writer'})
                payload['full_refresh_models'] = full
                unexpected = sorted(set(full) & set(incremental_models))
                if unexpected:
                    payload.update(status='failed', error='Incremental models executed a full refresh: '+', '.join(unexpected))
            elif self.engine.startswith('databricks-'):
                from services.databricks_openivm import statement_metrics
                payload.update(statement_metrics(statements, self.batch_num), kind='query_history')
                if self.engine == 'databricks-enzyme':
                    from services.databricks_enzyme_pipeline_events import collect_pipeline_events
                    events = collect_pipeline_events(self.batch_num, schemas=[self.connection.schema],
                                                     connection=self.connection.handle)
                    # Keep update IDs and raw events; only current-batch updates
                    # enter this export, even though the backend API is cumulative.
                    for pipeline in events['pipelines']:
                        pipeline['updates_without_timestamp'] = [u for u in pipeline['updates']
                                                                 if u.get('creation_time') is None]
                        pipeline['updates'] = [u for u in pipeline['updates']
                                               if u.get('creation_time') is not None
                                               and u['creation_time'] >= self.started_at_ms]
                        pipeline.pop('pipeline_level_events', None)
                    events['update_count'] = sum(len(p['updates']) for p in events['pipelines'])
                    events['event_count'] = sum(u['event_count'] for p in events['pipelines'] for u in p['updates'])
                    payload['pipeline_events'] = events
                    if (events['pipeline_count'] != len(self.models) or not events['update_count']
                            or any(p.get('compute',{}).get('status') != 'ok'
                                   or not p['updates'] or p['updates_without_timestamp'] or p.get('events_truncated')
                                   or p.get('updates_truncated') for p in events['pipelines'])):
                        payload['status'] = 'partial'
            elif self.engine in ('feldera', 'dbspnet'):
                if self.engine == 'feldera':
                    from services.feldera_client import get_stats
                else:
                    from services.dbspnet_client import get_stats
                metrics = get_stats()
                if not metrics:
                    raise RuntimeError('pipeline metrics unavailable after drain')
                payload.update(status='ok', kind='pipeline_metrics', metrics=metrics,
                               metrics_scope='cumulative snapshot after batch',
                               step_profile_status='not_supported',
                               reason='pipeline statistics are not per-model refresh step timings')
            else:
                payload.update(status='not_supported', kind='model_commands',
                               reason='engine has no OpenIVM refresh-step exporter; available model command times retained')
        except Exception as error:
            logging.exception('Fabric-DS %s batch %s profiling failed', self.engine, self.batch_num)
            payload.update(status='failed', error=str(error))
        return payload

    def _workload_view(self, view):
        qualified = str(view).replace('`','').replace('"','')
        prefix, _, name = qualified.rpartition('.')
        if (name or qualified) not in self.models:
            return False
        if self.engine == 'duckdb-openivm' or not prefix:
            return True
        namespace = self.connection.namespace
        return prefix == namespace or prefix.endswith('.'+namespace)
