import pyarrow as pa
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
from datetime import datetime
from utils.pipeline.Enums import Checkpoint

PIPELINE_CHECKPOINT_SCHEMA = pa.schema([
    pa.field("id", pa.string()),
    pa.field("pipeline", pa.string()),
    pa.field("status", pa.string()),
    pa.field("start_time", pa.string()),
    pa.field("update_time", pa.string()),
    pa.field("storage_path", pa.string()),
    pa.field("stage_source", pa.string()),
    pa.field("namespace", pa.string()),
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
    def persist(pipeline, status, start_time, update_time, storage_path, stage_source, namespace):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""
        try:
            tbl = PipelineCheckpoint._get_table()
            PipelineCheckpoint.migrate(tbl)

            rows_to_insert = [{ 
                'pipeline': pipeline, 'status': status, 'namespace': namespace,
                'start_time': start_time, 'update_time': update_time, 
                'storage_path': storage_path, 'stage_source': stage_source
            }]

            tbl.add(rows_to_insert)
            #if tbl.version % 100 == 0: PipelineCheckpoint.compact_metadata()

        except Exception as e:
            print(f"PipelineCheckpoint Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineCheckpoint persist Failed: {str(e)}')
        return pipeline, storage_path, start_time


    @staticmethod
    def update(pipeline, delayed_pipeline, storage_path, start_time, update_status):
        """Update pipeline checkpoint - In case there is a delayed pipeline competing for the same storage is now passes as active"""

        try:
            tbl, cp = PipelineCheckpoint._get_table(), Checkpoint
            PipelineCheckpoint.migrate(tbl)

            if delayed_pipeline:
                [status, strt_time, updte_time] = [cp.TAKING_CONTROL, cp.TIME_FROM_BASTION, cp.TIME_UNSET]
                filter = f"status='{cp.DELAY}' AND pipeline='{delayed_pipeline}'"
                tbl.update(where=f"{filter}", values_sql={ 'status': f"'{status}'", 'update_time': f"'{updte_time}'", 'start_time': f"'{strt_time}'" })

            filter = f"start_time='{start_time}' AND storage_path='{storage_path}' AND pipeline='{pipeline}'"
            tbl.update(where=f'{filter}', values_sql={ 'status': f"'{update_status}'", 'update_time': f"'{datetime.now().timestamp()}'" })

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

            new_fields = {}

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'pipeline_checkpoint.{col} added')
                else:
                    print(f'pipeline_checkpoint.{col} already exists — skipped')

        except Exception as e:
            print(f'pipeline_checkpoint migration failed: {e}')    


    @staticmethod
    def compact_metadata(older_than_days=30):
        from datetime import timedelta
        tbl = PipelineCheckpoint._get_table()
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ PipelineCheckpoint compacted")