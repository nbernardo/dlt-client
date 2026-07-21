import concurrent.futures
import dlt
from typing import Callable, Optional
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from utils.metastore.PipelineCheckpoint import PipelineCheckpoint
from utils.pipeline.Enums import Checkpoint
import duckdb
import os
from utils.logging.pipeline_logger_config import handle_pipeline_log
import traceback
from flask import current_app
import sys
import io


def on_pipeline_finished(future_object, runner, params: dict, triggers_cb, logger):
    try:
        load_info = future_object.result()
        print("\n[Callback Hook] Success!")
        print(load_info)
        params = { **params, 'cp_status': Checkpoint.DONE_DWH }

        PipelineCheckpoint.update(params.get('pipeline'), params=params)
        #Runs the pipeline trigger in case it exists
        triggers_cb()
    except Exception as error:
        traceback.print_exc()
        print(f"\n[Callback Hook] Error: {error}")
    finally:
        runner.shutdown() 
        return


class PipelineDWPhaseRunner:
    def __init__(self, source_db_path, dest_conn_wrapper, pipeline_name, dataset_name):
        """ 
        Initializes the async wrapper.
        @param source_db_path: The filesystem string path to your source file (e.g., 'path/data.duckdb')
        @param dest_conn_wrapper: The SQLAlchemy Connection object returned from raw_connection()
        """
        self.source_db_path = source_db_path      
        self.dest_conn_wrapper = dest_conn_wrapper
        self.pipeline_name = pipeline_name
        self.dataset_name = dataset_name
        self.params = None
        self.logger = None
        self.context = None
        self.pipeline = None
        self.params = None
        self.exec_id = None
        self.incr_fields = None
        
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._future: Optional[concurrent.futures.Future] = None

        self.app = current_app._get_current_object()
        

    def _clear_stale_schema(self):
        import json
        state_path = os.path.expanduser(f"~/.dlt/pipelines/{self.pipeline_name}/state.json")
        if os.path.exists(state_path):
            with open(state_path) as f:
                state = json.load(f)
            state["default_schema_name"] = None
            state["schema_names"] = []
            with open(state_path, "w") as f:
                json.dump(state, f, indent=2)


    def _run_pipeline(self, tables, pks, source_schema=None):

        with self.app.app_context():
            original_write = self._capture_stdout(
                    lambda msg: handle_pipeline_log(f'[Data Warehouse ingest] {msg}', self.logger, context=self.context)
                )

            os.environ["SCHEMA__NAMING"] = "direct"
            skma = source_schema[0] if type(source_schema) == list else source_schema

            try:
                con = duckdb.connect(self.source_db_path, read_only=True)
                tbls_fltr = str([f'{t}' for t in tables]).strip('[]')
                tbls = con.execute(f"SELECT table_name from information_schema.tables WHERE table_name in ('1',{tbls_fltr})").fetchall()
                tbls = list({t[0] for t in tbls})

                def make_resource(table, pk, incr_field = None):
                    def _resource():
                        batch_size, offset = 10_000, 0
                        msg = f'Injesting {table} to the Data Warehouse'
                        handle_pipeline_log(msg, self.logger, context=self.context)
                        while True:
                            rows = con.execute(f'SELECT * FROM "{skma}"."{table}" LIMIT {batch_size} OFFSET {offset}').fetchall()
                            if not rows: break
                            columns = [desc[0] for desc in con.description]
                            yield [dict(zip(columns, row)) for row in rows]
                            offset += batch_size

                    [_resource.__name__, _resource.__qualname__] = [table, table]
                    resource = dlt.resource(_resource, name=table, merge_key='_e2e_pk')
                    if incr_field != None and incr_field != '':
                        resource.apply_hints(incremental=dlt.sources.incremental('_e2e_update_date'))
                    return resource

                @dlt.resource(name='fk_map')
                def read_fk_map():
                    rows = con.execute('SELECT * FROM "dwhperformance_meta"."fk_map"').fetchall()
                    if not rows: return
                    columns = [desc[0] for desc in con.description]
                    batch_size = 10_000
                    for i in range(0, len(rows), batch_size):
                        yield [dict(zip(columns, row)) for row in rows[i:i + batch_size]]

                self._clear_stale_schema()

                dataset_name = self.dataset_name[0] if type(self.dataset_name) == list else self.dataset_name
                dest = dlt.destinations.duckdb(credentials=DuckDbCredentials(self.dest_conn_wrapper))
                pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name=dataset_name, destination=dest, progress='log')

                try:
                    fk_resource = read_fk_map()
                    fk_resource.apply_hints(primary_key=['table_name', 'fk_col', 'ref_table', 'ref_col'])
                    fk_pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name='dwhperformance_meta', destination=dest)
                    fk_pipeline.run(fk_resource, write_disposition='merge')
                except:
                    pass

                resources, count = [], 0
                incr_fields = str(self.incr_fields).strip('[]')
                incremental_load_field = incr_fields.split(',')
                for table in tbls:
                    i = tables.index(table) if table in tables else -1
                    pk = pks[i] if i >= 0 else []
                    incr_field = None
                    if(self.incr_fields != None and not(incr_fields in ['None',''])):
                        incr_field = incremental_load_field[count] if incremental_load_field[count].strip() != '' else None
                    resources.append(make_resource(table, pk, incr_field))
                    count = count + 1

                @dlt.source(name="dynamic_source")
                def build_source(): return resources

                result = pipeline.run(build_source(), write_disposition={ 'disposition': 'merge', 'strategy': 'scd2' })
                con.close()
                msg = f'Completed Data Warehouse data ingestions'
                handle_pipeline_log(msg, self.logger, context=self.context)
                #self.context.emit_ppsuccess(exec_id=self.exec_id)
                self.context.emit_error(error='success', exec_id=self.exec_id)

                return result
            
            except Exception as err:
                self.params['cp_status'] = Checkpoint.FAILED_INGEST_DW
                PipelineCheckpoint.update(self.pipeline, params=self.params)
                error = traceback.format_exc()
                f'Error while ingesting to Datawarehouse: {str(error)}'
                handle_pipeline_log(f'Error while ingesting to Datawarehouse: {str(error)}', self.logger, error=True, context=self.context)

            finally:
                self._restore_stdout(original_write)


    def _capture_stdout(self, callback):
        original_write = sys.stdout.write
        def patched_write(text):
            if text.strip(): callback(text)
            return original_write(text)
        sys.stdout.write = patched_write
        return original_write

    def _restore_stdout(self, original_write):
        sys.stdout.write = original_write


    def run_async(self, tables, pks, source_schema: Optional[str] = None, callback: Optional[Callable] = None) -> concurrent.futures.Future:
        if self._future and not self._future.done():
            msg = 'A pipeline execution task is already running in the background.'
            handle_pipeline_log(msg, context=self.context)
            raise RuntimeError(msg)

        self._future = self._executor.submit(self._run_pipeline, tables, pks, source_schema)
        
        if callback:
            self._future.add_done_callback(callback)
            
        return self._future


    def is_running(self) -> bool:
        return self._future is not None and not self._future.done()


    def shutdown(self):
        self._executor.shutdown(wait=True) 


    def run(namespace, data_source, refs):

        msg = 'Preprocessing Stage to Datawarehouse data ingestion'
        handle_pipeline_log(msg, refs.get('logger'), context=refs.get('context'))
        job_tag, triggers_cb = refs.get('job_tag'), refs.get('triggers', lambda: None)

        import schedule
        schedule.clear(job_tag)
        if not data_source: return

        import platform
        from utils.duckdb_util import DuckdbUtil
        from controller.pipeline import BasePipeline
        
        dwname = data_source.split('_for_',1)[-1]
        tables, pks, dataset, incr_fields = refs['dest_tables'], refs['tables_pks'], refs['dataset_name'], refs['incr_fields']
        
        if type(tables) == str: 
            tables = tables.strip('[]').replace(' ','').split(',') if tables.__contains__(',') else [tables.strip('[]')]

        if type(dataset) == str: 
            dataset = dataset.strip('[]').replace(' ','').split(',') if dataset.__contains__(',') else [dataset.strip('[]')]

        if type(pks) == str: 
            pks = pks.strip('[]').replace(' ','').split(',') if pks.__contains__(',') else [pks.strip('[]')]
        
        tables = [table.split('.')[-1] if str(table).__contains__('.') else table for table in tables]
        data_source = data_source[-1] if str(data_source).__contains__('/') else data_source
        
        sep = '/' if platform.system() != 'Windows' else '\\\\'
        db_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}'
        source_db = f"{db_path}{data_source}.duckdb"

        dest_native_conn = DuckdbUtil.get_connection_for(f'{db_path}{dwname}.duckdb')
        runner = PipelineDWPhaseRunner(source_db, dest_native_conn, dwname, dataset)

        [params, runner.logger, runner.context, runner.exec_id] = [refs.get('params'), refs.get('logger'), refs.get('context'), refs.get('exec_id')]
        [runner.pipeline, runner.params, runner.incr_fields] = params.get('pipeline'), params, incr_fields

        cb = lambda future: on_pipeline_finished(future, runner, params, triggers_cb, runner.logger)

        runner.run_async(tables, pks, dataset, callback=cb)