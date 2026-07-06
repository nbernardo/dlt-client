import pyarrow as pa
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
from datetime import datetime


PIPELINE_TRIGGER_SCHEMA = pa.schema([
    pa.field("id", pa.string()),
    pa.field("leader_pipeline", pa.string()), # pipeline the will dispatch the trigger
    pa.field("order", pa.string()), # order ot the trigger (e.g. 1, 2, ...). Which also defines execution order
    pa.field("pipeline", pa.string()), # pipeline script path
    pa.field("unity", pa.string()), # trigger time unity (Sec, Min, Hour)
    pa.field("time", pa.string()), # trigger execution time which get cobined with the unity (e.g. 1Hour)
    pa.field("status", pa.string()), # trigger status (e.g. active, unactive)
    pa.field("namespace", pa.string()), # account tenant which owns the trigger
])

table = 'pipeline_trigger'

class PipelineTrigger:

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_table() -> Table:
        """Opens or creates the LanceDB pipeline_checkpoint table"""
        try:
            return PipelineTrigger._get_lance_conn().open_table(table)
        except Exception:
            try:
                return PipelineTrigger._get_lance_conn().create_table(table, schema=PIPELINE_TRIGGER_SCHEMA)
            except Exception:
                return PipelineTrigger._get_lance_conn().open_table(table)    


    @staticmethod
    def persist(namespace, leader_pipeline, settings):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""
        try:
            from utils.pipeline.Enums import Trigger
            tbl = PipelineTrigger._get_table()
            PipelineTrigger.migrate(tbl)

            for setting in settings:

                pipeline, unity = setting['ppline'], setting['triggerValue']
                time, order = setting['timeUnit'], setting['order']
                sttus = setting.get('status', Trigger.STATUS_ACTIVE)

                is_existing_trigger = PipelineTrigger.find_all(namespace, leader_pipeline, order=order)
                if(len(is_existing_trigger) > 0):
                    filter = f"namespace='{namespace}' AND order='{order}' AND leader_pipeline='{leader_pipeline}'"
                    tbl.update(where=f'{filter}', values_sql={ 'status': f"'{sttus}'", 'unity': f"'{unity}'", 'time': f"'{time}'", 'pipeline': f"'{pipeline}'" })
                else:
                    rows_to_insert = [{ 
                        'pipeline': pipeline, 'leader_pipeline': leader_pipeline, 'status': sttus, 
                        'unity': str(unity), 'time': time, 'namespace': namespace, 'order': order 
                    }]
                    tbl.add(rows_to_insert)
            #if tbl.version % 100 == 0: PipelineTrigger.compact_metadata()

        except Exception as e:
            print(f"Pipeline Trigger Update Failed: {str(e)}")
            raise RuntimeError(f'PipelineTrigger persist Failed: {str(e)}')
        
        return


    @staticmethod
    def find_all(namespace, leader_pipeline, pipeline = None, order = None):
        """Update pipeline_trigger"""

        try:
            tbl = PipelineTrigger._get_table()
            PipelineTrigger.migrate(tbl)

            filter = f"leader_pipeline='{leader_pipeline}' AND namespace='{namespace}'"
            if pipeline:
                filter += f" AND pipeline='{pipeline}'"
            if order:
                filter += f" AND order='{order}'"

            records = tbl.search().where(f'{filter}').select(['pipeline', 'namespace', 'status', 'unity', 'time']).to_list()

            return records if records else []

        except Exception as e:
            print(f"Error while fetching PipelineTrigger: {str(e)}")
            raise RuntimeError(f'Error while fetching PipelineTrigger: {str(e)}')


    def migrate(tbl):
        """This is used to add a new pipeline_trigger field in case it didn't exist"""
        try:
            existing_cols = tbl.schema.names

            new_fields = {}

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'pipeline_trigger.{col} added')
                else:
                    print(f'pipeline_trigger.{col} already exists — skipped')

        except Exception as e:
            print(f'pipeline_trigger migration failed: {e}')    


    @staticmethod
    def compact_metadata(older_than_days=30):
        from datetime import timedelta
        tbl = PipelineTrigger._get_table()
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ PipelineTrigger compacted")
    ...