from datetime import datetime
import json
import re
import pyarrow as pa
from utils.duckdb_util import DuckdbUtil
import duckdb
from utils.db.lancedb import LanceConnectionFactory
from services.modeling.SemanticModel import SemanticModel
import sys

CATALOG_SCHEMA = pa.schema([
    pa.field("pipeline_run_id", pa.string()),
    pa.field("namespace", pa.string()),
    pa.field("original_table_name", pa.string()),
    pa.field("table_name", pa.string()),
    pa.field("source_store", pa.string()),
    pa.field("dest_store", pa.string()),
    pa.field("pipeline", pa.string()),
    pa.field("original_column_name", pa.string()),
    pa.field("column_name", pa.string()),
    pa.field("data_type", pa.string()),
    pa.field("ingested_at", pa.string()),
    # semantic-specific
    pa.field("semantic_concept", pa.string()),
    pa.field("confidence_score", pa.float32()),
    pa.field("description", pa.string()),
    pa.field("source", pa.string()),
    pa.field("validated", pa.int32()),
    pa.field("validated_by", pa.string()),
    pa.field("validated_at", pa.string()),
    # vector embedding
    pa.field("embedding", pa.list_(pa.float32(), 384)),
])


class DataCatalog:
    """LanceDB-backed catalog store. Writes via LanceDB (MVCC), reads via DuckDB SQL."""

    @staticmethod
    def _get_lance_conn(dbs_path=None) -> str:
        return LanceConnectionFactory.get(dbs_path)


    @staticmethod
    def _get_table(dbs_path=None):
        """Opens or creates the LanceDB catalog table"""
        db = DataCatalog._get_lance_conn(dbs_path)

        try:
            return db.open_table('column_catalog')
        except Exception:
            try:
                return db.create_table('column_catalog', schema=CATALOG_SCHEMA)
            except Exception:
                return db.open_table('column_catalog')


    @staticmethod
    def _get_duckdb_conn(dbs_path=None) -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB connection with a catalog view over the LanceDB files."""
        lance_path = f'{dbs_path}catalog.lance' if dbs_path else f'{DuckdbUtil.workspacedb_path}/catalog.lance'
        con = duckdb.connect()
        con.execute("LOAD lance")
        con.execute(f"""
            CREATE VIEW column_catalog AS
            SELECT * FROM '{lance_path}/column_catalog.lance'
        """)
        return con


    @staticmethod
    def _build_catalog_row(
        pipeline_run_id, namespace, orig_table_name, table_name,
        source_clean, dest_name, pipeline_name, now,
        orig_name, column_name, data_type, column_version, is_deleted
    ) -> dict:
        return {
            "pipeline_run_id": pipeline_run_id,
            "namespace": namespace,
            "original_table_name": orig_table_name,
            "table_name": table_name,
            "source_store": source_clean,
            "dest_store": dest_name,
            "pipeline": pipeline_name,
            "ingested_at": now,
            "original_column_name": orig_name,
            "column_name": column_name,
            "data_type": data_type,
            "column_version": column_version,
            "is_deleted": is_deleted,
        }


    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None, load_info=None, table_to_schema_map={}):
        """Persists column catalog to LanceDB. Concurrent writes via MVCC — This is called from the pipeline run itself"""
        pipeline_name = pipeline.pipeline_name
        now = datetime.now().isoformat()
        namespace = pipeline_name.split('_at_')[0]
        dbs_path = None if dbs_path is None else f'{dbs_path}/dbs/files/'
        dest_name = DataCatalog.get_destination(pipeline)
        source_clean = table_source.replace('"', '')
        pipeline_run_id = str(getattr(pipeline._last_trace, 'transaction_id', ''))

        all_updates = []
        for table_name, table_meta in pipeline.default_schema.tables.items():
            if table_name.startswith('_dlt_'): continue

            active_dlt_cols = {
                name: info for name, info in table_meta.get("columns", {}).items()
                if not name.startswith("_dlt_")
            }

            orig_table_name = table_to_schema_map.get(table_name, table_name) \
                if isinstance(table_to_schema_map, dict) else table_name

            all_updates.append((table_name, active_dlt_cols, orig_table_name))

        con, tbl = None, None
        try:

            tbl = DataCatalog._get_table(dbs_path)
            con = DataCatalog._get_duckdb_conn(dbs_path)
            
            existing = con.execute("""
                SELECT
                    table_name, original_column_name,
                    data_type, column_version,
                    is_deleted, column_name
                FROM column_catalog
                WHERE pipeline = ?
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY table_name, original_column_name
                    ORDER BY column_version DESC
                ) = 1
            """, (pipeline_name,)).fetchall()

            db_map = {}
            for row in existing:
                table, orig_col, dtype, version, deleted, col_name = row
                db_map.setdefault(table, {})[orig_col] = {
                    "data_type": dtype, "column_version": version, "is_deleted": deleted, "column_name": col_name,
                }

            rows_to_insert = []

            for table_name, active_dlt_cols, orig_table_name in all_updates:
                table_state = db_map.get(table_name, {})
                processed_orig_names = set()

                for name, info in active_dlt_cols.items():
                    orig_name = info.get("name", name)
                    new_type = info["data_type"]
                    processed_orig_names.add(orig_name)
                    last_state = table_state.get(orig_name)

                    if not last_state:
                        rows_to_insert.append(DataCatalog._build_catalog_row(
                            pipeline_run_id, namespace, orig_table_name, table_name,
                            source_clean, dest_name, pipeline_name, now,
                            orig_name, orig_name, new_type, 1, 0
                        ))
                    else:
                        if last_state["data_type"] != new_type or last_state["is_deleted"]:
                            rows_to_insert.append(DataCatalog._build_catalog_row(
                                pipeline_run_id, namespace, orig_table_name, table_name,
                                source_clean, dest_name, pipeline_name, now,
                                orig_name, orig_name, new_type,
                                int(last_state["column_version"]) + 1, 0
                            ))

                for orig_name, last_state in table_state.items():
                    if orig_name not in processed_orig_names and not last_state["is_deleted"]:
                        rows_to_insert.append(DataCatalog._build_catalog_row(
                            pipeline_run_id, namespace, orig_table_name, table_name,
                            source_clean, dest_name, pipeline_name, now,
                            orig_name, last_state["column_name"], last_state["data_type"],
                            int(last_state["column_version"]) + 1, 1
                        ))

            if rows_to_insert:
                new_rows = [r for r in rows_to_insert if r['column_version'] == 1 and r['is_deleted'] == 0]
                rows_to_insert = SemanticModel.get_semantic_model(rows_to_insert, new_rows, dbs_path)
                rows_to_insert = SemanticModel.get_embeddings(rows_to_insert)
                
                tbl.add(rows_to_insert)
                if tbl.version % 100 == 0:
                    DataCatalog.compact_catalog(dbs_path)

        except Exception as e:
            print(f"Catalog Evolution Update Failed: {str(e)}")

        finally:
            if con: con.close()
            sys.exit(0) # Gracefully terminates the sub-process


    @staticmethod
    def get_pipeline_datacatalog(pipeline: str, dbs_path=None, display_fields = False):
        try:
            con = DataCatalog._get_duckdb_conn(dbs_path)
            more_fields = ''
            if display_fields:
                more_fields = ',data_type, is_deleted, column_version, semantic_concept, confidence_score, validated, source'
            result = con.execute(f"""
                SELECT column_name AS name, table_name, source_store {more_fields}
                FROM column_catalog
                WHERE pipeline = ?
            """, (pipeline.replace('-', '_'),)).fetchall()
            con.close()
            if(display_fields == False):
                return [{"name": r[0], "table_name": r[1], "source_store": r[2]} for r in result]
            
            return [
                {
                    'name': r[0], 'table_name': r[1], 'source': r[2], 'type': r[3], 'deleted': r[4], 'original_semantic': r[6],
                    'version': r[5], 'semantic': r[6], 'confidence': r[7], 'validated': r[8], 'sem_source': r[9], 'original_source': r[9]
                }  for r in result
            ]
        except Exception as err:
            if str(err).__contains__('Extension'):
                print(f'ERROR: Duckdb missing Extension - The lancedb extension for Duckdb is not installed: {err}')
                print(f'Please install this extentions according to the documentation on https://duckdb.org/community_extensions/extensions/lance')
                return []
            else:
                print(f'Error while loading catalog: {err}')
            return err


    @staticmethod
    def get_destination(pipeline):
        creds = getattr(pipeline.destination.configuration, 'credentials', None) \
            or pipeline.destination.config_params
        if hasattr(creds, 'password'): creds.password = '************'
        if isinstance(creds, dict):
            destination_str = json.dumps({k: str(v) for k, v in creds.items() if k != 'password'})
        else:
            destination_str = json.dumps({
                k: str(getattr(creds, k)) for k in dir(creds)
                if not k.startswith('_') and not callable(getattr(creds, k)) and k != 'password'
            })
        return re.sub(r':([^@]+)@', ':***@', destination_str)


    @staticmethod
    def compact_catalog(dbs_path=None, older_than_days=30):
        from datetime import timedelta
        db = DataCatalog._get_lance_conn(dbs_path)
        tbl = db.open_table('column_catalog')
        tbl.cleanup_old_versions(older_than=timedelta(days=older_than_days))
        tbl.compact_files()
        print("✅ Catalog compacted")