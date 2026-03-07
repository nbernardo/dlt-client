from datetime import datetime
import sqlite3
import json
import re
import os
from utils.duckdb_util import DuckdbUtil

class MetaStore:
    """SQLite-backed catalog store for pipeline column metadata."""

    @staticmethod
    def _get_conn(dbs_path=None, db_name=None) -> sqlite3.Connection:
        ## This is a connection factory
        path = f'{dbs_path}{db_name}.db' if dbs_path else f'{DuckdbUtil.workspacedb_path}/{db_name}.db'
        con = MetaStore.init_catalog_table(path)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA synchronous = NORMAL")
        con.execute("PRAGMA cache_size = -64000")  # 64MB
        con.execute("PRAGMA temp_store = MEMORY")
        return con


    @staticmethod
    def init_catalog_table(path=None) -> sqlite3.Connection:
        """Creates SQLite main catalog DB. No-op if already exists. Call once at startup."""

        con = sqlite3.connect(path)
        if os.path.exists(path): return con

        try:
            con.execute("""
                CREATE TABLE column_catalog (
                    namespace TEXT,
                    table_name TEXT,
                    source_store TEXT,
                    dest_store TEXT,
                    source_file TEXT,
                    pipeline TEXT,
                    ingested_at TEXT,
                    original_column_name TEXT,
                    column_name TEXT,
                    data_type TEXT,
                    column_version INTEGER,
                    is_deleted INTEGER,
                    is_current INTEGER
                )
            """)
            con.execute("CREATE INDEX idx_pipeline_table ON column_catalog(pipeline, table_name)")
            con.execute("CREATE INDEX idx_orig_col ON column_catalog(original_column_name)")
            con.execute("CREATE INDEX idx_version ON column_catalog(column_version)")
            con.commit()

            return con
        finally:
            con.close()


    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None):
        """Creates the catalog for the calling pipeline. Runs in a separate thread."""
        [pipeline_name, now] = [pipeline.pipeline_name, datetime.now().isoformat()]

        namespace = pipeline_name.split('_at_')[0]
        dbs_path = dbs_path if dbs_path is None else f'{dbs_path}/dbs/files/'
        con = MetaStore._get_conn(dbs_path, 'catalog')
        dest_name = MetaStore.get_destination(pipeline)
        source_clean = table_source.replace('"', '')

        try:
            MetaStore.create_catalog_table(con)

            for table_name, table_meta in pipeline.default_schema.tables.items():
                if table_name.startswith('_dlt_'): continue

                rows = con.execute("""
                    SELECT t1.original_column_name, t1.data_type, t1.column_version,
                           t1.is_deleted, t1.column_name
                    FROM column_catalog t1
                    INNER JOIN (
                        SELECT original_column_name, MAX(column_version) AS max_version
                        FROM column_catalog
                        WHERE table_name = ? AND pipeline = ?
                        GROUP BY original_column_name
                    ) t2 ON  t1.original_column_name = t2.original_column_name
                         AND t1.column_version        = t2.max_version
                    WHERE t1.table_name = ? AND t1.pipeline = ?
                """, (table_name, pipeline_name, table_name, pipeline_name)).fetchall()

                db_map = {row["original_column_name"]: dict(row) for row in rows}

                active_dlt_cols = {
                    name: info for name, info in table_meta.get("columns", {}).items()
                    if not name.startswith("_dlt_")
                }

                updates = []
                processed_orig_names = set()

                for name, info in active_dlt_cols.items():
                    orig_name = info.get("name", name)
                    norm_name = MetaStore.get_normalized_name_selective(dest_name, name)
                    new_type  = info["data_type"]
                    processed_orig_names.add(orig_name)

                    last_state = db_map.get(orig_name)

                    if not last_state:
                        updates.append((namespace, table_name, source_clean, dest_name, pipeline_name, now, orig_name, orig_name, new_type, 1, 0))
                    else:
                        if last_state['data_type'] != new_type or last_state['is_deleted']:
                            new_version = int(last_state['column_version']) + 1
                            updates.append((namespace, table_name, source_clean, dest_name, pipeline_name, now, orig_name, orig_name, new_type, new_version, 0))

                for orig_name, last_state in db_map.items():
                    if orig_name not in processed_orig_names and not last_state['is_deleted']:
                        new_version = int(last_state['column_version']) + 1
                        updates.append((namespace, table_name, source_clean, dest_name, pipeline_name, now, orig_name, last_state['column_name'], last_state['data_type'], new_version, 1))

                if updates:
                    con.executemany("""
                        INSERT INTO column_catalog
                        (namespace, table_name, source_store, dest_store, pipeline, ingested_at,
                         original_column_name, column_name, data_type, column_version, is_deleted)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, updates)
            con.commit()

        except Exception as e:
            con.rollback()
            print(f"Catalog Evolution Update Failed: {e}")

        finally:
            con.close()


    @staticmethod
    def get_normalized_name_selective(destination_type: str, column_name: str) -> str:
        if not column_name: return "unnamed_column"

        target = destination_type.lower()

        if target == 'bigquery': return column_name
        if target in ['postgres', 'postgresql', 'athena', 'redshift']: return column_name.lower()
        if target == 'snowflake': return column_name.upper()

        return column_name


    @staticmethod
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


    @staticmethod
    def get_pipeline_metadata(pipeline: str):
        try:
            con    = MetaStore._get_conn(db_name='catalog')
            result = con.execute(
                "SELECT column_name as name, table_name, source_store FROM column_catalog WHERE pipeline = ?", (pipeline.replace('-','_'),)
            ).fetchall()
            con.close()
            return [dict(row) for row in result]
        except Exception as err:
            print('Error while loading catalog '+str(err))
            return err
    

    def create_catalog_table(con):
        exists = con.execute("""
                SELECT 1 FROM sqlite_master WHERE type='table' AND name='column_catalog'
            """).fetchone()
        
        if not exists:
            con.execute("""
                CREATE TABLE IF NOT EXISTS column_catalog (
                    namespace TEXT,
                    table_name TEXT,
                    source_store TEXT,
                    dest_store TEXT,
                    source_file TEXT,
                    pipeline TEXT,
                    ingested_at TEXT,
                    original_column_name TEXT,
                    column_name TEXT,
                    data_type TEXT,
                    column_version INTEGER,
                    is_deleted INTEGER,
                    is_current INTEGER
                )
            """)
            con.execute("CREATE INDEX idx_pipeline_table ON column_catalog(pipeline, table_name)")
            con.execute("CREATE INDEX idx_orig_col ON column_catalog(original_column_name)")
            con.execute("CREATE INDEX idx_version ON column_catalog(column_version)")
            con.commit()