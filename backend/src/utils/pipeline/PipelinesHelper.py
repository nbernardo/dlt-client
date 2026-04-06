from utils.logging.pipeline_logger_config import PipelineLogger as PL
from polars.dataframe import DataFrame
from dlt.common.schema.schema import Schema 

PipelineLogger = PL

def set_dlt_pipeline_schema_naming():
    import os
    os.environ["SCHEMA__NAMING"] = "direct"


set_dlt_pipeline_schema_naming()


def parse_aggregation(df: DataFrame, aggregation_list: list) -> DataFrame:

    aggregs = {}
    prev_aggregatio_field = None

    for aggreg in aggregation_list:
        if type(aggreg) != dict: continue

        if prev_aggregatio_field != None and prev_aggregatio_field != aggreg['field']:
            df_aggreg = df.group_by(prev_aggregatio_field).agg(
                aggregs[prev_aggregatio_field]
            )
            df = df.join(df_aggreg, on=prev_aggregatio_field, how="left")

        prev_aggregatio_field = aggreg['field']
        if not(prev_aggregatio_field in aggregs):
            aggregs[prev_aggregatio_field] = []
        aggregs[prev_aggregatio_field].append(aggreg['aggreg'])

    if prev_aggregatio_field != None:
        df_aggreg = df.group_by(prev_aggregatio_field).agg(
            aggregs[prev_aggregatio_field]
        )
        df = df.join(df_aggreg, on=prev_aggregatio_field, how="left")

    return df


from utils.metastore.meta_storage import MetaStore

class PipelineHelper:

    @staticmethod
    def parse_aggregation(df: DataFrame, aggregation_list: list) -> DataFrame:
        return parse_aggregation(DataFrame, aggregation_list)
    

    @staticmethod
    def wrapup_run(
        catalog_table_path: str, 
        src_path=None, 
        pipeline=None, 
        info=None, 
        additionals={}
    ):

        dest = additionals['dest']
        big_query = additionals['big_query']
        db_name = additionals['db_name']
        meta = additionals['meta']
        tbls = additionals['tbls']
        table_name = pipeline.pipeline_name.split('_at_', 1)[1]
        MetaStore.persist_catalog(catalog_table_path, src_path, pipeline, info, additionals)
        PipelineHelper._create_analytics_storage(dest, big_query, table_name, db_name, meta, tbls)


    @staticmethod
    def _create_analytics_storage(db_path, ready_query, domain_table, db_name, meta, tbls):
        import threading
        import duckdb
        import sys

        def run_transform(db_path, domain_table, big_query, done_event):
            try:
                with duckdb.connect(db_path.config_params['credentials']) as con:
                    con.execute(f"SET search_path = '{db_name}';")
                    
                    created_ghosts = []
                    for table_name in tbls:
                        table_name = table_name.split('.')[1] if table_name.__contains__('.') else table_name
                        exists = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]
                        
                        if not exists:
                            cols = meta[table_name]
                            null_cols = ", ".join([f"CAST(NULL AS VARCHAR) AS {c}" for c in cols])
                            
                            con.execute(f"CREATE VIEW {table_name} AS SELECT {null_cols} WHERE 1=0")
                            created_ghosts.append(table_name)
                            print(f"ghost: Created temporary view for missing table '{table_name}'")

                    con.execute("SET threads TO 4;")
                    con.execute("SET max_memory TO '4GB';")                    
                    con.execute(f"CREATE OR REPLACE TABLE {domain_table} AS {big_query}")

                    for view_name in created_ghosts:
                        con.execute(f"DROP VIEW {view_name}")

            except Exception as err:
                print(f'Error on running big table generation {str(err)}')

            finally:
                done_event.set()

        done_event = threading.Event()
        t = threading.Thread(target=run_transform, args=(db_path, domain_table, ready_query, done_event))
        t.start()

        while not done_event.is_set():
            done_event.wait(timeout=10.0)

        t.join()
        print('BI Table Created (Balanced Mode).')
        sys.exit(0) # Gracefully terminates the sub-process




