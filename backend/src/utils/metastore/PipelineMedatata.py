import pyarrow as pa
from utils.duckdb_util import DuckdbUtil
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
import asyncio

PIPELINE_METADATA_SCHEMA = pa.schema([
    pa.field("pipeline_run_id", pa.string()),
    pa.field("namespace", pa.string()),
    pa.field("source_secret_name", pa.string()),
    pa.field("dest_secret_name", pa.string()),
    pa.field("source_type", pa.string()),
    pa.field("dest_type", pa.string()),
    pa.field("pipeline", pa.string())
])


class PipelineMedatata:
    """LanceDB-backed catalog store. Writes via LanceDB (MVCC), reads via DuckDB SQL."""

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_table() -> Table:
        """Opens or creates the LanceDB pipeline_metadata table"""
        try:
            return PipelineMedatata._get_lance_conn().open_table('pipeline_metadata')
        except Exception:
            try:
                return PipelineMedatata._get_lance_conn().create_table('pipeline_metadata', schema=PIPELINE_METADATA_SCHEMA)
            except Exception:
                return PipelineMedatata._get_lance_conn().open_table('pipeline_metadata')


    @staticmethod
    def _get_duckdb_conn() -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection with a catalog view over the LanceDB files."""
        lance_path = f'{DuckdbUtil.workspacedb_path}/catalog.lance'
        con = duckdb.connect()
        con.execute("LOAD lance")
        con.execute(f"CREATE VIEW pipeline_metadata AS SELECT * FROM '{lance_path}/pipeline_metadata.lance'")
        return con


    @staticmethod
    def persist_metadata(namespace = None, pipeline=None, details={}):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""

        try:
            tbl = PipelineMedatata._get_table()
            rows_to_insert = [{
                'namespace': namespace, 'source_secret_name': details['source_secret'], 'source_type': details['source_type'],
                'pipeline': pipeline, 'dest_secret_name': details['destination_secret'], 'dest_type': details['destination_type']
            }]

            tbl.add(rows_to_insert)
            if tbl.version % 100 == 0: PipelineMedatata.compact_metadata()

        except Exception as e:
            print(f"Catalog Evolution Update Failed: {str(e)}")


    @staticmethod
    def get_pipeline_metadata(pipeline: str):
        try:
            con = PipelineMedatata._get_duckdb_conn()
            return con.execute(f"""
                SELECT pipeline_run_id, namespace, source_secret_name, dest_secret_name, pipeline, source_type, dest_type
                FROM pipeline_metadata WHERE pipeline = '{pipeline}'
            """).fetchone()

        except Exception as err:
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/community_extensions/extensions/lance')
                return []
            else:
                print(f'Error while loading catalog: {err}')
            return err


    @staticmethod
    def compact_metadata(older_than_days=30):
        from datetime import timedelta
        tbl = PipelineMedatata._get_table()
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ Catalog compacted")

    
    @staticmethod
    def get_pipeline_source_destination_type(namespace):
        return PipelineMedatata._get_duckdb_conn().execute("""
                SELECT JSON_GROUP_ARRAY(
                    JSON_OBJECT('sourceType', source_type, 'destType', dest_type, 'pipeline', pipeline, 'sourceSecretName', source_secret_name, 'destSecretName', dest_secret_name)
                )
                FROM pipeline_metadata WHERE namespace = ?""", [namespace]).fetchone()[0]