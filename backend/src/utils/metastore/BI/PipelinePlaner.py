import pyarrow as pa
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
from datetime import datetime
from utils.duckdb_util import DuckdbUtil

PIPELINE_PLAN_SCHEMA = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('user', pa.string()),
    pa.field('plan_setting', pa.string()),
    pa.field('namespace', pa.string()),
    pa.field('pipeline_lbl', pa.string()),
    pa.field('created_at', pa.string()),
    pa.field('updated_at', pa.string()),
])


class PipelinePlaner:

    table_name = 'pipeline_plan'

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_pipeline_plan() -> Table:
        """Opens or creates the LanceDB pipeline_plan table"""
        try:
            return PipelinePlaner._get_lance_conn().open_table(PipelinePlaner.table_name)
        except Exception:
            try:
                return PipelinePlaner._get_lance_conn().create_table(PipelinePlaner.table_name, schema=PIPELINE_PLAN_SCHEMA)
            except Exception:
                return PipelinePlaner._get_lance_conn().open_table(PipelinePlaner.table_name)


    @staticmethod
    def create_new_plan(namespace = None, settings = None):
        import json
        now = datetime.now().isoformat()

        try:
            tbl = PipelinePlaner._get_pipeline_plan()
            PipelinePlaner._migrate(tbl)

            rows_to_insert = [
                { 
                    'user': '', 'namespace': namespace, 'plan_setting': json.dumps(settings), 'id': LanceConnectionFactory.generate_id(), 
                    'created_at': now, 'pipeline_lbl': settings.get('pipeline_lbl')
                }
            ]
            tbl.add(rows_to_insert)
            if tbl.version % 100 == 0: PipelinePlaner.compact_metadata()

        except Exception as e:
            print(f"Pipeline plan Failed while saving: {str(e)}")
            print()

    
    @staticmethod
    def _migrate(tbl): pass


    @staticmethod
    def get_plans(namespace = None):
        try:
            tbl = PipelinePlaner._get_pipeline_plan()
            return tbl.search().where(f"namespace = '{namespace}'").to_list()
            
        except Exception as e:
            print(f"Pipeline plan fetch failed: {str(e)}")