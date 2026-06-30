import concurrent.futures
import dlt
from typing import Callable, Optional
from dlt.sources.sql_database import sql_database
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials
from utils.metastore.PipelineCheckpoint import PipelineCheckpoint
from utils.pipeline.Enums import Checkpoint
import duckdb
import os

def on_pipeline_finished(future_object, runner, params: dict):
    try:
        load_info = future_object.result()
        print("\n[Callback Hook] Success!")
        print(load_info)
        params = { **params, 'cp_status': Checkpoint.DONE_DWH }

        PipelineCheckpoint.update(params.get('pipeline'), params=params)
    except Exception as error:
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
        
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._future: Optional[concurrent.futures.Future] = None


    def _run_pipeline(self, tables, pks, source_schema=None):
        os.environ["SCHEMA__NAMING"] = "direct"
        skma = source_schema[0] if type(source_schema) == list else source_schema
        engine, creds = "sqlalchemy", f"duckdb:///{self.source_db_path}"

        con = duckdb.connect(self.source_db_path)
        tbls_fltr = str([f'{t}' for t in tables]).strip('[]')
        tbls = con.execute(f"SELECT table_name from information_schema.tables WHERE table_name in ('1',{tbls_fltr}) ").fetchall()
        tbls = list({t[0] for t in tbls})
        con.close()

        source = sql_database(credentials=creds,backend=engine,schema=skma, reflection_level='minimal').with_resources(*tbls)

        for idx in range(len(tbls)):
            table = tbls[idx]
            i = tables.index(table) if table in tables else -1
            if i >= 0:
                source.resources[table].apply_hints(primary_key=pks[i])

        dataset_name = self.dataset_name[0] if type(self.dataset_name) == list else self.dataset_name
        dest = dlt.destinations.duckdb(credentials=DuckDbCredentials(self.dest_conn_wrapper))
        pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name=dataset_name, destination=dest,)

        # It might not have dwhperformance_meta when data source is a file (e.g. csv)
        try:
            # Runs the unique column ingestion for every table being ingested
            source1 = sql_database(credentials=creds,backend=engine,schema='dwhperformance_meta', reflection_level='minimal').with_resources('fk_map')
            fk_pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name='dwhperformance_meta', destination=dest,)
            
            source1.resources['fk_map'].apply_hints(primary_key=['table_name','fk_col','ref_table','ref_col'])
            fk_pipeline.run(source1, write_disposition="merge")
        except:
            pass

        # Runs the ingestion of the actuall datawarehouse tables
        return pipeline.run(source, write_disposition="merge")


    def run_async(self, tables, pks, source_schema: Optional[str] = None, callback: Optional[Callable] = None) -> concurrent.futures.Future:
        if self._future and not self._future.done():
            raise RuntimeError("A pipeline execution task is already running in the background.")

        self._future = self._executor.submit(self._run_pipeline, tables, pks, source_schema)
        
        if callback:
            self._future.add_done_callback(callback)
            
        return self._future


    def is_running(self) -> bool:
        return self._future is not None and not self._future.done()


    def shutdown(self):
        self._executor.shutdown(wait=True) 


    def run(namespace, data_source, refs, job_tag):

        import schedule
        schedule.clear(job_tag)
        if not data_source: return

        import platform
        from utils.duckdb_util import DuckdbUtil
        from controller.pipeline import BasePipeline
        
        dwname = data_source.split('_for_',1)[-1]
        tables, pks, dataset_name = refs['dest_tables'], refs['tables_pks'], refs['dataset_name']
        
        if type(tables) == str: tables = [tables.strip('[]')]
        if type(dataset_name) == str: dataset_name = [dataset_name.strip('[]')]
        if type(pks) == str: pks = [pks.strip('[]')]
        
        tables = [table.split('.')[-1] if str(table).__contains__('.') else table for table in tables]
        data_source[0].split('_for_',1)

        data_source = data_source[-1] if str(data_source).__contains__('/') else data_source
        
        sep = '/' if platform.system() != 'Windows' else '\\\\'
        db_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}'
        source_db = f"{db_path}{data_source}.duckdb"

        dest_native_conn = DuckdbUtil.get_connection_for(f'{db_path}{dwname}.duckdb')

        runner = PipelineDWPhaseRunner(source_db, dest_native_conn, dwname, dataset_name)
        params = refs.get('params')
        runner.run_async(tables, pks, dataset_name, callback=lambda future: on_pipeline_finished(future, runner, params))