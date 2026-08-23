import pyarrow as pa
from utils.duckdb_util import DuckdbUtil
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from lancedb import Table
import traceback

PIPELINE_METADATA_SCHEMA = pa.schema([
    pa.field("pipeline_run_id", pa.string()),
    pa.field("namespace", pa.string()),
    pa.field("source_secret_name", pa.string()),
    pa.field("dest_secret_name", pa.string()),
    pa.field("source_type", pa.string()),
    pa.field("dest_type", pa.string()),
    pa.field("pipeline", pa.string()),
    pa.field("stage_storage", pa.string()),
    pa.field("dest_tables", pa.string()),
    pa.field("tables_pks", pa.string()),
])


class PipelineMedatata:
    """LanceDB-backed catalog store. Writes via LanceDB (MVCC), reads via DuckDB SQL."""

    @staticmethod
    def _get_lance_conn() -> str:
        return LanceConnectionFactory.get()


    @staticmethod
    def _get_table() -> Table:
        """Opens or creates the LanceDB pipeline_metadata table"""
        tbl = None
        try:
            tbl = PipelineMedatata._get_lance_conn().open_table('pipeline_metadata')
        except Exception:
            try:
                tbl =  PipelineMedatata._get_lance_conn().create_table('pipeline_metadata', schema=PIPELINE_METADATA_SCHEMA)
            except Exception:
                tbl =  PipelineMedatata._get_lance_conn().open_table('pipeline_metadata')
        PipelineMedatata._migrate_pipeline_metadata(tbl)
        return tbl


    @staticmethod
    def _get_duckdb_conn(include_catalog = False, db_path = None) -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection with a catalog view over the LanceDB files."""
        # Forces pipeline catalog table creation
        tbl = PipelineMedatata._get_table()
        PipelineMedatata._migrate_pipeline_metadata(tbl)

        lance_path = f'{db_path if db_path != None else DuckdbUtil.workspacedb_path}/catalog.lance'
        con = duckdb.connect()
        con.execute("LOAD lance")
        con.execute(f"CREATE VIEW pipeline_metadata AS SELECT * FROM '{lance_path}/pipeline_metadata.lance'")
        if(include_catalog):
            con.execute(f"CREATE VIEW column_catalog AS SELECT dest_store, pipeline FROM '{lance_path}/column_catalog.lance'")
        return con


    @staticmethod
    def update_metadata_and_pipeline_plan(
        namespace = None, pipeline=None, details={}, dataset_name: str = '', 
        short_query: str = '', pipline_plan_id = None, params: dict = None
    ):
        from utils.metastore.BI.PipelinePlan import PipelinePlan
        PipelineMedatata.persist_metadata(namespace, pipeline, details, dataset_name, short_query, params)
        if(pipline_plan_id != None):
            PipelinePlan.mark_as_ran(pipline_plan_id)



    @staticmethod
    def persist_metadata(
        namespace = None, pipeline=None, details={}, dataset_name: str = '', 
        short_query: str = '', params: dict = None
    ):
        """Persists the pipeline metadata — This is called from the pipeline run itself"""
        try:
            tbl, ref_ppline = PipelineMedatata._get_table(), None
            PipelineMedatata._migrate_pipeline_metadata(tbl)

            if details['existing_wd'] != None:
                dataset_name = details['existing_wd']
            if str(dataset_name).strip() == '':
                dataset_name = params.get('dataset_name') if params else details.get('dataset_name')

            dest_tables = params.get('dest_tables') if params else details.get('dest_tables')
            tables_pks = params.get('tables_pks') if params else details.get('tables_pks')
            domain_pipeline = params.get('domain_pipeline') if params else details.get('domain_pipeline')
            
            if params: dataset_name = params.get('dataset_name')

            if details.get('existing_wd') != None:
                if details.get('existing_wd','').__contains__('.'):
                    ref_ppline = details.get('existing_wd').split('.')[1]
            
            rows_to_insert = [{
                'namespace': namespace, 'source_secret_name': details.get('source_secret'), 'source_type': details.get('source_type'), 
                'stage_storage': details.get('stage_storage'), 'pipeline': pipeline, 'dest_secret_name': details.get('destination_secret'), 
                'dest_type': details['destination_type'], 'source_config': details.get('source_config'), 'domain_pipeline': domain_pipeline,
                'destination_config': details.get('destination_config'), 'dest_tables': dest_tables, 'tables_pks': tables_pks, 
                'reference_pipeline': ref_ppline, 'short_query': short_query, 'referenced_secrets': str(details.get('referenced_secrets')), 
                'dataset_name': dataset_name, 'incremental_field': details.get('incr_field'), 'db_src_name': details.get('db_src_name')
            }]

            tbl.add(rows_to_insert)
            if tbl.version % 100 == 0: PipelineMedatata.compact_metadata()

        except Exception as e:
            traceback.print_exc()
            print(f"Catalog Evolution Update Failed: {str(e)}")


    @staticmethod
    def update_metadata(namespace = None, pipeline=None, dataset_name: str = '', short_query: str = ''):
        """Update the pipeline metadata only touching the shortquery and the dataset_name"""
        try:
            tbl = PipelineMedatata._get_table()

            filter = f"pipeline='{pipeline}' AND namespace='{namespace}'"
            tbl.update(where=filter, values_sql={ 'short_query': f"'{short_query}'", 'dataset_name': f"'{dataset_name}'" })

        except Exception as e:
            print(f"Catalog Evolution Update Failed: {str(e)}")


    @staticmethod
    def get_pipeline_metadata(pipeline: str, namespace: str = None, db_path: str = None):
        try:
            con = PipelineMedatata._get_duckdb_conn(False, db_path)
            more_filter = f"and namespace = '{namespace}'" if namespace != None else ''
            result = con.execute(f"""
                SELECT 
                    pipeline_run_id, namespace, source_secret_name, dest_secret_name, pipeline, 
                    source_type, dest_type, dataset_name, stage_storage, dest_tables, tables_pks,
                    incremental_field, db_src_name
                FROM pipeline_metadata WHERE pipeline = ? {more_filter}
            """, [pipeline]).fetchone()
            con.close()
            return result

        except Exception as err:
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/docs/current/core_extensions/lance')
                return []
            else:
                print(f'Error while loading catalog: {err}')
            return err


    @staticmethod
    def get_domain_pipelines(namespace: str = None, db_path: str = None):
        try:
            con = PipelineMedatata._get_duckdb_conn(False, db_path)
            result = con.execute(f"SELECT pipeline, dataset_name, namespace, reference_pipeline FROM pipeline_metadata WHERE domain_pipeline::string IN ('true','1') AND namespace = ?", [namespace]).fetchall()
            con.close()
            return result

        except Exception as err:
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/docs/current/core_extensions/lance')
                return []
            else:
                print(f'Error while loading catalog: {err}')
            return err


    @staticmethod
    def get_stage_data(namespace: str, stage = 2, permisions: list = []):
        try:
            con = PipelineMedatata._get_duckdb_conn()
            result = con.execute(f'SELECT pipeline, dataset_name, namespace FROM pipeline_metadata WHERE domain_pipeline = ? AND namespace = ?', [str(stage), namespace]).fetchall()
            con.close()
            return result

        except Exception as err:
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/docs/current/core_extensions/lance')
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
        print("✅ Pipeline metadata compacted")

    
    @staticmethod
    def get_pipeline_source_destination_type(namespace):
        try:
            con = PipelineMedatata._get_duckdb_conn(include_catalog=True)
            result = con.execute("""
                    SELECT JSON_GROUP_ARRAY(
                        JSON_OBJECT(
                            'sourceType', source_type, 'destType', dest_type, 'pipeline', pipeline, 'sourceSecretName', source_secret_name, 
                            'destSecretName', dest_secret_name, 'destinationConfig', destination_config, 'referencedSecrets', referenced_secrets,
                            'datasetName', dataset_name
                        )
                    )
                    FROM pipeline_metadata WHERE namespace = ?""", [namespace]).fetchone()[0]
            con.close()
            return result
        except: None

    
    @staticmethod
    def get_pipeline_source_destination_meta(namespace):
        try:
            con = PipelineMedatata._get_duckdb_conn(include_catalog=True)
            result = con.execute("""
                    SELECT 
                        json_group_object(pipeline, metadata_list) as final_json
                    FROM (
                        SELECT pipeline, 
                            list({'source_type': source_type, 'dest_type': dest_type, 'connection_name': dest_secret_name, 'dbname': dataset_name }) as metadata_list
                        FROM pipeline_metadata WHERE namespace = ?
                        GROUP BY pipeline
                    )""", [namespace]).fetchone()[0]
            con.close()
            return result
        except: return None


    @staticmethod
    def _migrate_pipeline_metadata(tbl):
        """This is used to add a new metadata field in case it didn't exist"""
        try:
            existing_cols = tbl.schema.names

            new_fields = {
                'destination_config': "cast(null as string)", #added in Mar/22/2026
                'source_config': "cast(null as string)", #added in Mar/22/2026
                'referenced_secrets': "cast(null as string)", #added in Mar/22/2026
                'dataset_name': "cast(null as string)", #added in Mar/22/2026
                'big_query': "cast(null as string)", #added in Apr/05/2026
                'short_query': "cast(null as string)", #added in Apr/05/2026
                'domain_pipeline': "cast(null as string)", #added in Apr/05/2026
                'reference_pipeline': "cast(null as string)", #added in Jul/19/2026
                'incremental_field': "cast(null as string)", #added in Jul/19/2026
                'active': "cast(null as string)", #added in Jul/19/2026
                'db_src_name': "cast(null as string)", #added in Jul/20/2026
            }

            for col, expr in new_fields.items():
                if col not in existing_cols:
                    tbl.add_columns({col: expr})
                    print(f'pipeline_metadata.{col} added')
                else:
                    print(f'pipeline_metadata.{col} already exists — skipped')

        except Exception as e:
            print(f'pipeline_metadata migration failed: {e}')

    
    def get_pipeline_short_list(namespace):
        tbl = PipelineMedatata._get_table()
        result = tbl.search().where(f"namespace='{namespace}'").select(['pipeline','dataset_name']).to_list()
        return result
