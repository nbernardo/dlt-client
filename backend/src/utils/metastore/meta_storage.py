from utils.duckdb_util import DuckdbUtil
from duckdb import DuckDBPyConnection
from datetime import datetime
import polars as pl
import json
import re

class DuckDBMedaStore:
    """Simplified DuckDB store focusing on batch performance with auto-parsing."""
    
    def _get_conn(path = None) -> DuckDBPyConnection: return DuckdbUtil.get_meta_db_instance(path)

    @staticmethod
    def init_catalog_table(con):
        con.execute("""
            CREATE TABLE IF NOT EXISTS column_catalog (
                namespace VARCHAR,
                table_name VARCHAR,
                source_store VARCHAR,
                dest_store VARCHAR,
                source_file VARCHAR,
                pipeline VARCHAR,
                ingested_at TIMESTAMP,
                original_column_name VARCHAR,
                column_name VARCHAR,
                data_type VARCHAR,
                column_version INTEGER,
                is_deleted BOOLEAN,
                is_current BOOLEAN                    
            )
        """)
            

    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None):
        dbs_path = dbs_path if dbs_path is None else f'{dbs_path}/dbs/files/'
        con = DuckDBMedaStore._get_conn(dbs_path)
        DuckDBMedaStore.init_catalog_table(con)
        
        dest_name = DuckDBMedaStore.get_destination(pipeline)
        source_clean = table_source.replace('"', '')
        [pipeline_name, now] = [pipeline.pipeline_name, datetime.now()]

        try:
            for table_name, table_meta in pipeline.default_schema.tables.items():
                if table_name.startswith('_dlt_'): continue

                query = f"""
                    SELECT original_column_name, data_type, column_version, is_deleted, column_name
                    FROM column_catalog t1
                    WHERE table_name = '{table_name}' 
                    AND pipeline = '{pipeline_name}'
                    AND column_version = (
                        SELECT MAX(column_version) FROM column_catalog t2 
                        WHERE t1.original_column_name = t2.original_column_name 
                        AND t2.table_name = '{table_name}' AND t2.pipeline = '{pipeline_name}'
                    )
                """
                
                db_state = pl.from_arrow(con.execute(query).arrow())
                db_map = { row["original_column_name"]: row  for row in db_state.to_dicts()}

                active_dlt_cols = {
                    name: info for name, info in table_meta.get("columns", {}).items() 
                    if not name.startswith("_dlt_")
                }

                updates = []
                processed_orig_names = set()

                for name, info in active_dlt_cols.items():
                    orig_name = info.get("name", name)
                    norm_name = DuckDBMedaStore.get_normalized_name_selective(dest_name, name)
                    new_type = info["data_type"]
                    processed_orig_names.add(orig_name)

                    last_state = db_map.get(orig_name)

                    if not last_state:
                        updates.append((table_name, source_clean, dest_name, pipeline_name, now, orig_name, orig_name, new_type, 1, False))
                    else:
                        if last_state['data_type'] != new_type or last_state['is_deleted']:
                            new_version = int(last_state['column_version']) + 1
                            updates.append((table_name, source_clean, dest_name, pipeline_name, now, orig_name, orig_name, new_type, new_version, False))

                for orig_name, last_state in db_map.items():
                    if orig_name not in processed_orig_names and not last_state['is_deleted']:
                        new_version = int(last_state['column_version']) + 1
                        updates.append((table_name, source_clean, dest_name, pipeline_name, now, orig_name, last_state['column_name'], last_state['data_type'], new_version, True))

                if updates:
                    con.executemany("""
                        INSERT INTO column_catalog 
                        (table_name, source_store, dest_store, pipeline, ingested_at, 
                        original_column_name, column_name, data_type, column_version, is_deleted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, updates)

        except Exception as e:
            print(f"Polars Catalog Evolution Update Failed: {e}")


    def get_normalized_name_selective(destination_type: str, column_name: str) -> str:
        if not column_name: return "unnamed_column"

        target = destination_type.lower()

        if target == 'bigquery': return column_name 

        if target in ['postgres', 'postgresql', 'athena', 'redshift']:
            return column_name.lower()

        if target == 'snowflake': return column_name.upper()

        return column_name
    

    def get_destination(pipeline):
        creds = getattr(pipeline.destination.configuration, 'credentials', None) or pipeline.destination.config_params
        if hasattr(creds, 'password'): creds.password = '************'
        if isinstance(creds, dict):
            destination_str = json.dumps({k: str(v) for k, v in creds.items() if k != 'password'})
        else:
            destination_str = json.dumps({
                k: str(getattr(creds, k))
                for k in dir(creds)
                if not k.startswith('_') and not callable(getattr(creds, k)) and k != 'password'
            })
        return re.sub(r':([^@]+)@', ':***@', destination_str)


