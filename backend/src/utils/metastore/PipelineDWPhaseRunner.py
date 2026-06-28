import concurrent.futures
import dlt
from typing import Callable, Optional
from dlt.sources.sql_database import sql_database
from dlt.destinations.impl.duckdb.configuration import DuckDbCredentials

def handle_pipeline_finished(future_object, runner):
    try:
        load_info = future_object.result()
        print("\n[Callback Hook] Success!")
        print(load_info)
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
        
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._future: Optional[concurrent.futures.Future] = None


    def _run_pipeline(self, tables, pks, source_schema=None):
        source_instance = sql_database(
            credentials=f"duckdb:///{self.source_db_path}",
            backend="sqlalchemy",
            schema=source_schema
        ).with_resources(*tables)

        for table, pk in zip(tables, pks):
            if pk:
                source_instance.resources[table].apply_hints(primary_key=pk)

        dest = dlt.destinations.duckdb(credentials=DuckDbCredentials(self.dest_conn_wrapper))
        pipeline = dlt.pipeline(pipeline_name=self.pipeline_name, dataset_name=self.dataset_name, destination=dest,)

        return pipeline.run(source_instance, write_disposition="merge")


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


    def run(namespace, data_source, dwname, start_time, refs):

        import schedule
        import platform
        from utils.duckdb_util import DuckdbUtil
        from controller.pipeline import BasePipeline
        
        tables, pks, dataset_name = refs['dest_tables'], refs['tables_pks'], refs['dataset_name']
        tables = [table.split('.')[-1] if str(table).__contains__('.') else table for table in tables]

        schedule.clear(f'{start_time}_{dwname}')
        data_source = data_source[-1] if str(data_source).__contains__('/') else data_source
        
        sep = '/' if platform.system() != 'Windows' else '\\\\'
        db_path = f'{BasePipeline.folder}{sep}duckdb{sep}{namespace}{sep}'
        source_db = f"{db_path}{data_source}.duckdb"

        dest_native_conn = DuckdbUtil.get_connection_for(f'{db_path}{dwname}.duckdb')

        runner = PipelineDWPhaseRunner(source_db, dest_native_conn, dwname, dataset_name)
        runner.run_async(tables, pks, dataset_name, callback=lambda future: handle_pipeline_finished(future, runner))