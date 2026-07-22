import pyarrow as pa
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
from datetime import datetime
from utils.pipeline.Enums import Checkpoint

PIPELINE_CHECKPOINT_SCHEMA = pa.schema([
    pa.field("id", pa.string()), #Represents pipeline execution id when it runs
    pa.field("pipeline", pa.string()),
    pa.field("status", pa.string()),
    pa.field("start_time", pa.string()),
    pa.field("update_time", pa.string()),
    pa.field("storage_path", pa.string()),
    pa.field("stage_source", pa.string()),
    pa.field("namespace", pa.string()),
    pa.field("dest_tables", pa.string()),
    pa.field("tables_pks", pa.string()),
    pa.field("dataset_name", pa.string()),
])

table = 'pipeline_checkpoint'

class PipelineCheckpoint:


    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_table() -> Table:
        """Opens or creates the LanceDB pipeline_checkpoint table"""
        try:
            return PipelineCheckpoint._get_lance_conn().open_table(table)
        except Exception:
            try:
                return PipelineCheckpoint._get_lance_conn().create_table(table, schema=PIPELINE_CHECKPOINT_SCHEMA)
            except Exception:
                return PipelineCheckpoint._get_lance_conn().open_table(table)    


    @staticmethod
    def persist(pipeline, namespace, stage_source, params: dict):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""
        try:
            tbl = PipelineCheckpoint._get_table()
            PipelineCheckpoint.migrate(tbl)

            if(int(params.get('exp_backoff',1)) > 1 or params.get('manual_run')):
                PipelineCheckpoint.update(pipeline, None, params)
            else:
                rows_to_insert = [{ 
                    'id': params.get('exec_id'), 'pipeline': pipeline, 'status': params.get('state'), 'namespace': namespace, 
                    'start_time': params.get('start_time'), 'update_time': params.get('updt_time'), 'storage_path': params.get('dest_storage'), 
                    'stage_source': stage_source, 'dest_tables': params.get('dest_tables'), 'tables_pks': params.get('tables_pks'), 
                    'dataset_name': params.get('dataset_name'), 'manual_run_count': 0
                }]
                tbl.add(rows_to_insert)
                #if tbl.version % 100 == 0: PipelineCheckpoint.compact_metadata()

        except Exception as e:
            print(f"PipelineCheckpoint Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineCheckpoint persist Failed: {str(e)}')
        return pipeline, params.get('dest_storage'), params.get('start_time')


    @staticmethod
    def update(pipeline, delayed_pipeline = None, params = None, retry_count = None):
        """Update pipeline checkpoint - In case there is a delayed pipeline competing for the same storage is now passes as active"""

        try:
            retry_count, values = int(params.get('exp_backoff',1)) - 1, {}
            tbl, cp = PipelineCheckpoint._get_table(), Checkpoint
            PipelineCheckpoint.migrate(tbl)

            if delayed_pipeline:
                #[status, strt_time, updte_time] = [cp.TAKING_CONTROL, cp.TIME_FROM_BASTION, cp.TIME_UNSET]
                filter = f"status='{cp.DELAY}' AND pipeline='{delayed_pipeline}'"

            if(not params.get('manual_run')):
                filter = f"""
                    (start_time='{params.get('start_time')}' AND storage_path='{params.get('storage_path')}' AND pipeline='{pipeline}') 
                    or (id LIKE '{params.get('exec_id').strip()}%')"""
                values = { 
                    'status': f"'{params.get('cp_status')}'", 'update_time': f"'{datetime.now().timestamp()}'", 'retry_count': f"'{retry_count}'"
                }
                
            else:
                values['manual_run_count'] = 'manual_run_count + 1'
                values['status'] = f"'{cp.MANUAL}'"
                values['retry_count'] = "0"
                filter = f"id LIKE '{params.get('exec_id').strip()}%' AND status = '{cp.FAILED}'"

            res = tbl.update(where=filter, values_sql=values)

        except Exception as e:
            print(f"PipelineCheckpoint Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineCheckpoint update Failed: {str(e)}')


    @staticmethod
    def check_dest_storge_usage(storage_path, pipeline):
        """Update pipeline checkpoint"""

        try:
            tbl = PipelineCheckpoint._get_table()
            PipelineCheckpoint.migrate(tbl)
            filter = f"storage_path='{storage_path}' AND pipeline != '{pipeline}' AND status NOT IN ('{Checkpoint.DONE}', '{Checkpoint.DELAY}')"
            records = tbl.search().where(f'{filter}').select(['pipeline']).limit(1).to_list()

            return records[0]['pipeline'] if records else None

        except Exception as e:
            print(f"PipelineCheckpoint Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineCheckpoint update Failed: {str(e)}')


    @staticmethod
    def check_delayed_pipeline(storage_path, pipeline):
        """Update pipeline checkpoint"""

        try:
            tbl = PipelineCheckpoint._get_table()
            PipelineCheckpoint.migrate(tbl)
            query = f"storage_path='{storage_path}' AND pipeline != '{pipeline}' AND status NOT INT ('{Checkpoint.DELAY}','{Checkpoint.STAGED}')"
            records = tbl.search().where(query).select(['pipeline']).limit(1).to_list()

            return records[0]['pipeline'] if records else None

        except Exception as e:
            print(f"PipelineCheckpoint Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineCheckpoint update Failed: {str(e)}')


    def migrate(tbl):
        """This is used to add a new checkpoint field in case it didn't exist"""
        try:
            existing_cols = tbl.schema.names

            new_fields = {
                'retry_count': "cast(null as string)", #added in Jul/09/2026
                'manual_run_count': "cast(0 as integer)", #added in Jul/13/2026
            }

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'pipeline_checkpoint.{col} added')
                else:
                    print(f'pipeline_checkpoint.{col} already exists — skipped')

        except Exception as e:
            print(f'pipeline_checkpoint migration failed: {e}')    


    @staticmethod
    def get_run_history(namespace):

        from utils.duckdb_util import DuckdbUtil
        import json

        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"""
            SELECT cp.* FROM main.ppline_schedule ps RIGHT JOIN checkpoint cp ON ps.ppline_name = cp.pipeline
            WHERE cp.status = 'FAILED' and cp.namespace = '{namespace}'
        """
        # checkpoint variables creates the checkpoint table used in the query
        checkpoint = PipelineCheckpoint._get_table().to_arrow()
        result = cnx.query(query).df()

        return json.loads(result.to_json(orient="records"))


    @staticmethod
    def compact_metadata(older_than_days=30):
        from datetime import timedelta
        tbl = PipelineCheckpoint._get_table()
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ PipelineCheckpoint compacted")