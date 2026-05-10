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
    pa.field('updated_at', pa.string())
])


class PipelinePlan:

    table_name = 'pipeline_plan'

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_pipeline_plan() -> Table:
        """Opens or creates the LanceDB pipeline_plan table"""
        try:
            return PipelinePlan._get_lance_conn().open_table(PipelinePlan.table_name)
        except Exception:
            try:
                return PipelinePlan._get_lance_conn().create_table(PipelinePlan.table_name, schema=PIPELINE_PLAN_SCHEMA)
            except Exception:
                return PipelinePlan._get_lance_conn().open_table(PipelinePlan.table_name)


    @staticmethod
    def create_new_plan(namespace = None, settings = None, is_update = False, id = None):
        import json
        now = datetime.now().isoformat()

        try:
            tbl = PipelinePlan._get_pipeline_plan()
            PipelinePlan._migrate(tbl)

            rows_to_insert = [{ 
                'user': '', 'namespace': namespace, 'plan_setting': json.dumps(settings), 'id': LanceConnectionFactory.generate_id(), 
                'created_at': now, 'pipeline_lbl': settings.get('pipeline_lbl'), 'version': 1, 'processed': 0
            }]

            if(is_update):
                tbl.update(where=f'id={id}', values_sql={'plan_setting': f"'{json.dumps(settings)}'", 'version': 'version + 1', 'processed': '0'})
            else:
                tbl.merge_insert('id').when_not_matched_insert_all().execute(rows_to_insert)

            if tbl.version % 100 == 0: PipelinePlan.compact_metadata()

            return { 'error': False }

        except Exception as e:
            print(f"Pipeline plan Failed while saving: {str(e)}")
            return { 'error': True, 'result': f'Pipeline plan Failed while saving: {str(e)}' }


    @staticmethod
    def mark_as_ran(pipline_plan_id = None):
        now = datetime.now().isoformat()

        try:
            tbl = PipelinePlan._get_pipeline_plan()
            PipelinePlan._migrate(tbl)
            tbl.update(where=f'id={pipline_plan_id}', values_sql={ 'processed': '1', 'run_date': f"'{now}'" })

            if tbl.version % 100 == 0: PipelinePlan.compact_metadata()

        except Exception as e:
            print(f"Pipeline plan Failed while saving: {str(e)}")

    
    @staticmethod
    def _migrate(tbl): 
        """This is used to add a new metadata field in case it didn't exist"""
        try:
            existing_cols = tbl.schema.names
            new_fields = { 
                'version': 'cast(0 as bigint)', # Added on 05/08/2026
                'processed': 'cast(0 as bigint)', # Added on 05/08/2026
                'run_date': 'cast(null as string)' # Added on 05/08/2026
            }

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'pipeline_plan.{col} added')
                else:
                    print(f'pipeline_plan.{col} already exists — skipped')

        except Exception as e:
            print(f'pipeline_plan migration failed: {e}')


    @staticmethod
    def get_plans(namespace = None):
        try:
            tbl = PipelinePlan._get_pipeline_plan()
            return tbl.search()\
                    .select({ 'id': 'cast(id as string)', 'plan_setting': 'plan_setting', 'pipeline_lbl': 'pipeline_lbl' })\
                    .where(f"namespace = '{namespace}'").to_list()
            
        except Exception as e:
            print(f"Pipeline plan fetch failed: {str(e)}")