from utils.duckdb_util import DuckdbUtil
from duckdb import DuckDBPyConnection
from datetime import datetime
from dlt.common.normalizers.naming.snake_case import NamingConvention
import re

class DuckDBMedaStore:
    """Simplified DuckDB store focusing on batch performance with auto-parsing."""
    
    def _get_conn(path = None) -> DuckDBPyConnection: return DuckdbUtil.get_meta_db_instance(path)

    @staticmethod
    def init_catalog_table(con):
        con.execute("""
            CREATE TABLE IF NOT EXISTS column_catalog (
                table_name VARCHAR,
                source_store VARCHAR,
                dest_store VARCHAR,
                source_file VARCHAR,
                pipeline VARCHAR,
                ingested_at VARCHAR,
                original_column_name VARCHAR,
                column_name VARCHAR,
                data_type VARCHAR
            )
        """)
        

    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None):

        dbs_path = dbs_path if dbs_path is None else f'{dbs_path}/dbs/files/'
        con: DuckDBPyConnection = DuckDBMedaStore._get_conn(dbs_path)
        DuckDBMedaStore.init_catalog_table(con)
        dest_name = pipeline.destination.destination_name

        try:
            for table_name, table_meta in pipeline.default_schema.tables.items():
                if table_name.startswith('_dlt_'): continue

                file_name = table_meta.get("resource") or table_name

                catalog_entries = [
                    (
                        table_name,
                        table_source.replace('"', ''),
                        dest_name,
                        file_name,
                        pipeline.pipeline_name,
                        str(datetime.now()),
                        info.get("name", name),
                        DuckDBMedaStore.get_normalized_name_selective(dest_name, name),
                        info["data_type"]
                    )
                    for name, info in table_meta.get("columns", {}).items()
                    if "data_type" in info and not name.startswith("_dlt_")
                ]

                if not catalog_entries: continue

                con.execute(f"""
                    DELETE FROM column_catalog 
                    WHERE table_name = '{table_name}' 
                    AND source_store = '{table_source.replace(chr(34), '')}'
                    AND pipeline = '{pipeline.pipeline_name}'
                """)

                con.executemany("""
                    INSERT INTO column_catalog 
                    (table_name, source_store, dest_store, source_file, pipeline, ingested_at, original_column_name, column_name, data_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, catalog_entries)

        except Exception as e:
            print(f"DuckDB Batch Write Failed: {e}")
    


    def get_normalized_name_selective(destination_type: str, column_name: str) -> str:
        if not column_name:
            return "unnamed_column"

        target = destination_type.lower()

        if target == 'bigquery': return column_name 

        if target in ['postgres', 'postgresql', 'athena', 'redshift']:
            return column_name.lower()

        if target == 'snowflake': return column_name.upper()

        return column_name

