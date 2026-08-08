import concurrent.futures
import dlt
from typing import Callable, Optional
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from utils.metastore.PipelineCheckpoint import PipelineCheckpoint
from utils.pipeline.Enums import Checkpoint
import duckdb
import os
from utils.logging.pipeline_logger_config import handle_pipeline_log, PipelineLogger
import traceback
from flask import current_app
import sys
from pathlib import Path
from contextlib import contextmanager


def on_pipeline_finished(future_object, runner, params: dict, triggers_cb, logger, email_cb):
    try:
        load_info = future_object.result()
        print("\n[Callback Hook] Success!")
        print(load_info)
        params = { **params, 'cp_status': Checkpoint.DONE_DWH }

        PipelineCheckpoint.update(params.get('pipeline'), params=params)
        #Runs the pipeline trigger in case it exists
        if email_cb(runner.rec_count_per_table) != None:
            handle_pipeline_log(f'> Sent email', logger, context=runner.context)
        triggers_cb()
    except Exception as error:
        traceback.print_exc()
        print(f"\n[Callback Hook] Error: {error}")
    finally:
        if os.environ.get('KEEP_STAGE_DB',0) in [0,'0']:
            db_stage_file = Path(runner.source_db_path)
            db_stage_file.unlink(missing_ok=True)
            runner.shutdown()
        return


class PipelineDWPhaseRunner:

    @contextmanager
    def _capture_stdout(self):
        original_write = sys.stdout.write

        def patched_write(text):
            if text.strip():
                handle_pipeline_log(
                    text.strip(),
                    self.logger,
                    context=self.context,
                )
            return original_write(text)

        sys.stdout.write = patched_write
        try:
            yield
        finally:
            sys.stdout.write = original_write


    def __init__(self, source_db_path, dest_conn_wrapper, pipeline_name, dataset_name):
        """ 
        Initializes the async wrapper.
        @param source_db_path: The filesystem string path to your source file (e.g., 'path/data.duckdb')
        @param dest_conn_wrapper: The SQLAlchemy Connection object returned from raw_connection()
        """
        self.source_db_path = source_db_path
        self.wd_db_path = None
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
        self.rec_count_per_table = {}
        
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

            os.environ["SCHEMA__NAMING"] = "direct"
            skma = source_schema[0] if type(source_schema) == list else source_schema
            handle_pipeline_log(f'Connecting to stage source: {self.source_db_path}', self.logger, context=self.context)
            con = duckdb.connect(self.source_db_path, read_only=True)

            try:
                tbls_fltr = str([f'{t}' for t in tables]).strip('[]')
                fetch_table_query = f"SELECT table_name from information_schema.tables WHERE table_name in ('1',{tbls_fltr})"
                handle_pipeline_log(f'Fetch tables query: {fetch_table_query}', self.logger, context=self.context)
                tbls = con.execute(fetch_table_query).fetchall()
                handle_pipeline_log(f'Fetch tables query result: {str(tbls)}', self.logger, context=self.context)
                tbls = list({t[0] for t in tbls})
                handle_pipeline_log(f'Fetch tables query result parsed: {str(tbls)}', self.logger, context=self.context)

                def make_resource(table, pk, incr_field = None):
                    def _resource():
                        batch_size, offset = 10_000, 0
                        msg = f'Injesting {table} to the Data Warehouse'
                        handle_pipeline_log(msg, self.logger, context=self.context)

                        if isinstance(pk, list) and pk: order_col = pk[0]
                        elif isinstance(pk, str) and pk: order_col = pk
                        else: order_col = '1'

                        while True:
                            rows = con.execute(f'''
                                SELECT *, md5(concat_ws('|', COLUMNS(*))) AS _e2e_row_hash FROM "{skma}"."{table}"
                                ORDER BY {order_col} LIMIT {batch_size} OFFSET {offset}
                            ''').fetchall()

                            if not rows: break

                            columns = [desc[0] for desc in con.description]
                            yield [dict(zip(columns, row)) for row in rows]
                            offset += batch_size

                    [_resource.__name__, _resource.__qualname__] = [table, table]
                    resource = dlt.resource(_resource, name=table, primary_key='_e2e_pk', merge_key='_e2e_pk')
                    if incr_field != None and incr_field != '':
                        resource.apply_hints(incremental=dlt.sources.incremental(incr_field, on_cursor_value_missing='include'))
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
                
                import dlt.common.storages.file_storage as file_storage_module
                _original_delete = file_storage_module.FileStorage.delete

                def _safe_delete(self, file_path, *a, **kw):
                    full_path = os.path.join(self.storage_path, file_path) if not os.path.isabs(file_path) else file_path
                    if os.path.exists(full_path):
                        return _original_delete(self, file_path, *a, **kw)
                # Monkeypatching/replacing the original dlt delete method
                file_storage_module.FileStorage.delete = _safe_delete

                try:
                    fk_resource = read_fk_map()
                    fk_resource.apply_hints(primary_key=['table_name', 'fk_col', 'ref_table', 'ref_col'])
                    fk_pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name='dwhperformance_meta', destination=dest)
                    fk_pipeline.run(fk_resource, write_disposition='merge')
                except:
                    pass

                incr_fields, resources = str(self.incr_fields).strip('[]'), []
                incremental_load_field = incr_fields.split(',')

                for table in tbls:
                    self.rec_count_per_table[table] = 0
                    i = tables.index(table) if table in tables else -1
                    pk = pks[i] if i >= 0 else []
                    incr_field = None
                    if self.incr_fields is not None and not (incr_fields in ['None', '']):
                        incr_field = incremental_load_field[i] if i >= 0 and incremental_load_field[i].strip() != '' else None
                    resources.append(make_resource(table, pk, incr_field))

                @dlt.source(name="dynamic_source")
                def build_source(): return resources

                result = None
                pipeline.sync_destination(dataset_name=dataset_name)
                handle_pipeline_log(f'State after sync: {pipeline.state}', self.logger, context=self.context)
                pipeline.drop_pending_packages()
                handle_pipeline_log(f'State before run: {pipeline.state}', self.logger, context=self.context)

                with self._capture_stdout():
                    result = pipeline.run(build_source(), write_disposition={ 'disposition': 'merge', 'strategy': 'scd2', 'row_version_column_name': '_e2e_row_hash' })
                    count_per_table = pipeline.last_trace.last_normalize_info.row_counts if pipeline.last_trace.last_normalize_info else {}
                    self.rec_count_per_table = { **self.rec_count_per_table, **count_per_table }
                
                con.close()
                msg = f'Completed Data Warehouse data ingestions'
                handle_pipeline_log(msg, self.logger, context=self.context)
                #self.context.emit_ppsuccess(exec_id=self.exec_id)
                self.context.emit_error(error='success', exec_id=self.exec_id)
                return result
            
            except Exception as err:
                con.close()
                self.params['cp_status'] = Checkpoint.FAILED_INGEST_DW
                PipelineCheckpoint.update(self.pipeline, params=self.params)
                error = traceback.format_exc()
                f'Error while ingesting to Datawarehouse: {str(error)}'
                self.context.emit_error(error='Error', exec_id=self.exec_id)
                handle_pipeline_log(f'Error while ingesting to Datawarehouse: {str(error)}', self.logger, error=True, context=self.context)

            finally:
                con.close()



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

        try:
            msg = 'Preprocessing Stage to Datawarehouse data ingestion'
            handle_pipeline_log(msg, refs.get('logger'), context=refs.get('context'))
            job_tag, triggers_cb = refs.get('job_tag'), refs.get('triggers', lambda: None)
            send_email_cb = refs.get('email_cb', lambda: None)

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
            runner.wd_db_path = f'{db_path}{dwname}.duckdb'

            [params, runner.logger, runner.context, runner.exec_id] = [refs.get('params'), refs.get('logger'), refs.get('context'), refs.get('exec_id')]
            [runner.pipeline, runner.params, runner.incr_fields] = params.get('pipeline'), params, incr_fields

            cb = lambda future: on_pipeline_finished(future, runner, params, triggers_cb, runner.logger, send_email_cb)
            handle_pipeline_log(
                f'Tables, PKs and Incr-fields to ingest: Tables: {str(tables)}, Pks: {str(pks)}, Incr-fields: {str(incr_fields)}', 
                refs.get('logger'), context=refs.get('context')
            )
            runner.run_async(tables, pks, dataset, callback=cb)

        except Exception as err:
            handle_pipeline_log(
                f'Datawarehouse Phase runner exception {str(err)}', 
                refs.get('logger'), context=refs.get('context')
            )