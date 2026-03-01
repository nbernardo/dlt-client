from utils.duckdb_util import DuckdbUtil
from duckdb import DuckDBPyConnection
from datetime import datetime

class DuckDBMedaStore:
    """Simplified DuckDB store focusing on batch performance with auto-parsing."""
    
    def _get_conn(path = None) -> DuckDBPyConnection: return DuckdbUtil.get_meta_db_instance(path)

    @staticmethod
    def init_catalog_table(con):
        con.execute("""
            CREATE TABLE IF NOT EXISTS column_catalog (
                table_name VARCHAR,
                table_source VARCHAR,
                source_file VARCHAR,
                pipeline VARCHAR,
                ingested_at VARCHAR,
                original_column_name VARCHAR,
                column_name VARCHAR,
                data_type VARCHAR
            )
        """)
        

    @staticmethod
    def catalog_collect(pipeline, table_name: str) -> list:

        if(table_name.startswith('_dlt_')): return

        schema = pipeline.default_schema
        source_files = pipeline.state.get("source_files", {})

        table_meta = schema.tables.get(table_name, {})
        source_info = source_files.get(table_name, {})
        file_name = source_info.get("file_name") or table_meta.get("resource") or table_name
        original_columns = source_info.get("original_columns", [])

        catalog_entries = [
            (
                table_name,
                file_name,
                pipeline.pipeline_name,
                str(datetime.now()),
                original_columns[i] if i < len(original_columns) else info.get("name", name),
                name,
                info["data_type"]
            )
            for i, (name, info) in enumerate(table_meta.get("columns", {}).items())
            if "data_type" in info and not name.startswith("_dlt_")
        ]

        with pipeline.managed_state() as state:
            state["catalog"] = state.get("catalog", {})
            state["catalog"][table_name] = catalog_entries

        return catalog_entries


    @staticmethod
    def persist_catalog(table_source: str, dbs_path=None, pipeline=None, read_type = None):

        dbs_path = dbs_path if dbs_path is None else f'{dbs_path}/dbs/files/'
        con: DuckDBPyConnection = DuckDBMedaStore._get_conn(dbs_path)
        DuckDBMedaStore.init_catalog_table(con)


        try:
            schema = pipeline.default_schema
            if read_type != None:
                table_name = [table for table in list(schema.tables) if not(str(table).startswith('_dlt_'))]
                DuckDBMedaStore.simple_catalog_collect(pipeline, table_source, read_type, table_name[0])

            has_collected = bool(pipeline.state.get("catalog"))
            for table_name, table_meta in schema.tables.items():
                if table_name.startswith('_dlt_'): continue
                if has_collected:
                    catalog_entries = pipeline.state["catalog"].get(table_name, [])
                else:
                    file_name = table_meta.get("description") or table_meta.get("resource") or table_name
                    catalog_entries = [
                        (
                            table_name,
                            table_source.replace('"', ''),
                            file_name,
                            pipeline.pipeline_name,
                            str(datetime.now()),
                            info.get("name", name),
                            name,
                            info["data_type"]
                        )
                        for name, info in table_meta.get("columns", {}).items()
                        if "data_type" in info and not name.startswith("_dlt_")
                    ]

                if not catalog_entries: continue

                con.execute(f"""
                    DELETE FROM column_catalog 
                    WHERE table_name = '{table_name}' 
                    AND table_source = '{table_source.replace(chr(34), '')}'
                """)

                con.executemany("""
                    INSERT INTO column_catalog 
                    (table_name, table_source, source_file, pipeline, ingested_at, original_column_name, column_name, data_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, catalog_entries)

        except Exception as e:
            print(f"DuckDB Batch Write Failed: {e}")

    
    @staticmethod
    def simple_catalog_collect(pipeline, path: str, read_type, table_name):
        
        import polars as pl
        file_name, table_source = path.split('/')[-1], path

        if read_type == "parquet":
            file_schema = pl.read_parquet_schema(path)
            original_columns = file_schema.names()
            data_types = [str(t) for t in file_schema.dtypes()]
        else:
            sample_df = getattr(pl, f'read_{read_type}')(path, n_rows=0)
            original_columns = sample_df.columns
            data_types = [str(t) for t in sample_df.dtypes]

        catalog_entries = [
            (
                table_name,
                table_source,
                file_name,
                pipeline.pipeline_name,
                str(datetime.now()),
                original_col,
                name,
                data_type
            )
            for (original_col, data_type), (name, _) in zip(
                zip(original_columns, data_types),
                pipeline.default_schema.tables.get(table_name, {}).get("columns", {}).items()
            )
            if not original_col.startswith("_dlt_")
        ]

        with pipeline.managed_state() as state:
            state["catalog"] = state.get("catalog", {})
            state["catalog"][table_name] = catalog_entries