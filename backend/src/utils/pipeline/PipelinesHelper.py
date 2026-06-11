from utils.logging.pipeline_logger_config import PipelineLogger as PL
from polars.dataframe import DataFrame
from os import getenv as env
import sys
import threading
import duckdb
from duckdb import DuckDBPyConnection
from utils.pipeline.Enums import Checkpoint
from services.email.SimpleAPIMailer import SimpleAPIMailer
from datetime import datetime


PipelineLogger = PL
ChkPoint = Checkpoint

def set_dlt_pipeline_schema_naming():
    import os
    os.environ["SCHEMA__NAMING"] = "direct"


set_dlt_pipeline_schema_naming()

def prefx(table_name, type = None): return PipelineHelper.prefix_and_suffix_table(table_name, type)


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

        start_time = str(sys.stdin.readline())
        ppline_strt_dt, ppline_end_dt = datetime.fromtimestamp(float(start_time)), datetime.now()
        ppline_time = { 'ts': start_time, 'start': ppline_strt_dt, 'end': ppline_end_dt }

        [dest, db_name] = [additionals['dest'], additionals['db_name']]
        [meta, tbls] = [additionals['meta'], additionals['tbls']]
        [stage, big_query] = [additionals.get('stage', 0), additionals.get('big_query', '')]
        
        loads_ids = info.loads_ids if type(info.loads_ids) == list else []
        loads_ids_str = ','.join([f"'{val}'" for val in loads_ids])
        con: DuckDBPyConnection = duckdb.connect(dest.config_params['credentials'])

        # TODO: Implement the flow to activate/deactivate bigtable generation
        generate_big_table = False

        try:
            pipeline_name = pipeline.pipeline_name
            namespace, original_pipeline_name = pipeline_name.split('_at_', 1)
            
            [tbls, ts, skma] = [list(additionals['ddls'].keys()), additionals['ts'], additionals['db_name']]
            send_ppline_completion_email(con, original_pipeline_name, skma, ts, tbls, ppline_time)

            metadatas = MetaStore.get_pipeline_catalog(original_pipeline_name, namespace, src_path)
            MetaStore.persist_catalog(catalog_table_path, src_path, pipeline, info, additionals, len(metadatas) > 1 if metadatas else 0)

            if stage in [1,2,3]:
                PipelineHelper.add_tables_contrains(con, tbls, additionals, stage)

            if stage in [1,'1'] and generate_big_table:
                stage = int(stage)
                source_pipeline = pipeline.pipeline_name.split('_at_', 1)[1]
                table_name = additionals.get('analytics_storage', source_pipeline)
                big_table = PipelineHelper.prefix_and_suffix_table(table_name, stage)
                exists = con.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{big_table}'").fetchone()

                big_query = str(big_query).replace('SELECT\n',f"SELECT\n'{source_pipeline}' AS _e2e_intgrtion_src_,\n ",1)

                if exists != None:
                    ts = additionals['ts']
                    PipelineHelper._update_analytics_storage(con, big_query, big_table, db_name, meta, tbls, loads_ids_str, ts)
                else:
                    PipelineHelper._create_analytics_storage(con, big_query, big_table, db_name, meta, tbls)

        except Exception as err:
            print('Error on running here: ', str(err))

        finally:
            con.close()
            sys.exit(0) # Gracefully terminates the sub-process


    @staticmethod
    def add_tables_contrains(con, tbls, adtnls, optmz_typ):
        contraints = adtnls.get('rels')

        for tbl_name in tbls:
            table_path = tbl_name.split('.')
            tbl_name = table_path[1] if len(table_path) > 1 else tbl_name
            stg_tbl = tbl_name #prefx(tbl_name, optmz_typ)

            con.execute("CREATE SCHEMA IF NOT EXISTS dwhperformance_meta")
            tbl_schema = 'table_name VARCHAR, fk_col VARCHAR, ref_table VARCHAR, ref_col VARCHAR, PRIMARY KEY (table_name, fk_col)'
            con.execute(f"CREATE TABLE IF NOT EXISTS dwhperformance_meta.fk_map ({tbl_schema})")

            con.executemany(
                """ INSERT OR REPLACE INTO dwhperformance_meta.fk_map (table_name, fk_col, ref_table, ref_col) VALUES (?, ?, ?, ?) """,
                [(stg_tbl, r["columns"][0], r["referred_table"], r["referred_columns"][0])  for r in contraints[tbl_name]]
            )
            

    @staticmethod
    def _create_analytics_storage(con, ready_query, big_table, db_name, meta, tbls):

        def run_transform(con, big_table, big_query, done_event):
            try:
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

                con.execute(f'SET threads TO {env('AN_TOTAL_THREADS')};')
                con.execute(f"SET max_memory TO '{env('AN_MAX_MEMORY')}';")                    
                con.execute(f"CREATE OR REPLACE TABLE {big_table} AS {big_query}")

                for view_name in created_ghosts:
                    con.execute(f"DROP VIEW {view_name}")

            except Exception as err:
                print(f'Error on running big table generation {str(err)}')

            finally:
                done_event.set()

        done_event = threading.Event()
        t = threading.Thread(target=run_transform, args=(con, big_table, ready_query, done_event))
        t.start()

        while not done_event.is_set():
            done_event.wait(timeout=10.0)

        t.join()
        print('BI Table Created (Balanced Mode).')


    @staticmethod
    def _update_analytics_storage(con: DuckDBPyConnection, ready_query, big_table, db_name, meta, tbls, loads_ids_str, ts):
        '''
            This method handles Schema evolution for existing bigtable on the Datawarehouse or
            pipeline re-run (run count > 1) targeting a big table that was generated in the first run
        ''' 

        def run_transform(con: DuckDBPyConnection, big_table, big_query, done_event, ts):
            try:
                con.execute(f"SET search_path = '{db_name}';")

                # new_columns -> Extract the new columns to be added in the big_table for Schema evolution
                # insert_cols_str -> Extract the new fields separated by comma to which data should be inserted
                # were_clause -> Where clause filters
                new_columns, insert_cols_str, where_clause = PipelineHelper.get_new_columns_and_filter(con, meta, tbls, big_table, loads_ids_str, ts)
                new_cols_str = '\n'.join([f"ALTER TABLE {big_table} ADD COLUMN {c} {str(type).replace('()','')};" for c, type in new_columns.items()])
                insert_query = f'INSERT INTO {big_table} {big_query}'

                if len(new_cols_str):
                    new_cols_str = str(new_cols_str).lower().replace('jsonb','json')
                    con.execute(f'BEGIN;\n{new_cols_str}\nCOMMIT;')

                con.execute(f'SET threads TO {env('AN_TOTAL_THREADS')};')
                con.execute(f"SET max_memory TO '{env('AN_MAX_MEMORY')}';")
                con.execute(insert_query)

            except Exception as err:
                print(f'Error on running big table update {str(err)}')

            finally:
                done_event.set()

        done_event = threading.Event()
        t = threading.Thread(target=run_transform, args=(con, big_table, ready_query, done_event, ts))
        t.start()

        while not done_event.is_set():
            done_event.wait(timeout=10.0)

        t.join()
        print('BI Table Created (Balanced Mode).')


    def get_new_columns_and_filter(con, meta, tbls, big_table, loads_ids_str, time_stmp):
        '''
            This extracts the new columns to be inserted as well as the where 
            clause of the data to be filtered to load into the bigtable
        '''
        columns_to_add, insert_columns, were_clauses = {}, '', []
        fact_tbl = None
        for tbl in tbls:
            cols = ''
            tbl = tbl.split('.')[1]
            if fact_tbl == None: fact_tbl = tbl
            # To filter only the records of the last load_id registered by dlt
            were_clauses.append(f"{tbl}._dlt_load_id IN ({loads_ids_str})")

            for c, _ in meta[tbl].items():
                insert_columns += f'{tbl}_{c},'
                cols += f"('{tbl}_{c}'),"

            query = f'''SELECT list.col_name FROM ( VALUES {cols[0:-1]} ) AS list(col_name) LEFT JOIN information_schema.columns AS info 
                        ON list.col_name = info.column_name AND info.table_name = '{big_table}' WHERE info.column_name IS NULL;
                    '''
            
            result = con.query(query).fetchall()
            
            if(len(result) > 0):
                new_cols = str(result).replace('[','').replace(']','').replace("',)",'').replace("('",'').replace(' ','').split(',')
                
                # Append the new columns names accordingly (format tablename_columnname)
                # every single column is being mapped to the type from the data source
                for c in new_cols:
                    real_column_name = c.split(tbl+'_',1)[1]
                    columns_to_add[c] = meta[tbl][real_column_name]                
        
        insert_columns = insert_columns[0:-1]
        # {tbls[0]}._e2e_ts >= '{time_stmp} -> Will only fetch the records loaded form the timestamp which
        # the pipeline ran started, combined with dlt-load it'll prevent from loading data from another pipeline 
        where_clauses_str = ''#f"{fact_tbl}._e2e_ts >= '{time_stmp}'" AND (" + (' OR '.join(were_clauses)) + ")"
        return columns_to_add, insert_columns, where_clauses_str


    def short_query(big_query):
        return f'SELECT * FROM {big_query.split('FROM ')[1].replace('\n',' ')}' if big_query.__contains__('FROM ') else ''
    

    def prefix_and_suffix_table(table_name, type = None, source_ppline = ''):
        if type in [1, None]:
            return f'_e2e_domain_{table_name}_data_'
        if type == 2:
            return f'_e2e_domain_{table_name}_stage_1_'
        if type == 3:
            return f'_e2e_domain_{table_name}_stage_2_'
        if type == 100:
            return f'{table_name}_e2e_from_{source_ppline}'
        


def send_ppline_completion_email(
    con: DuckDBPyConnection,
    pipeline_name: str,
    skma: str,
    ts: str,
    tbls: list,
    ppline_time: dict
):
    from internationalization.email import labels

    ppline_strt_dt, ppline_end_dt, start_time = ppline_time.get('start'), ppline_time.get('end'), ppline_time.get('ts')
    count_query = '\n UNION \n'.join([f"SELECT '{tbl}' as tbl, COUNT(*) as count FROM {skma}.{tbl} WHERE _e2e_ts = '{ts}'" for tbl in tbls])
    total_record_per_table = con.execute(count_query).fetchall()
    
    style, row_sep = 'style="padding: 0 8px; text-align: center;"', '=' * 50
    intl = labels[env('APP_LANG')]

    table_main_lbl = intl['TBL_MAIN_HEAD']
    table_lbl = intl['TBL_TABLE_HEAD_LBL']
    records_lbl = intl['TBL_REC_HEAD_LBL']
    st_dt_lbl = intl['PPLINE_START']
    end_dt_lbl = intl['PPLINE_END']
    start_time_lbl = intl['PPLINE_TIME']
    sbjct_prfix = intl['SBJCT_PREFX']
    sbjct_sffix = intl['SBJCT_SFFIX']
    _success_execution = intl['PPLINE_SUCCESS']

    tbls_email_content = ''.join([f'<tr><td {style}>{tbl[0]}</td><td {style}>{tbl[1]}</td></tr>' for tbl in list(total_record_per_table)])
    tbls_email_content = f'<tr style="font-weight: bold; text-align: center;"><td>{table_lbl}</td><td>{records_lbl}</td></tr>{tbls_email_content}'
    tbls_email_content = f'<tr><td style="font-weight: bold;  text-align: center;" colspan="2">{table_main_lbl}</td></tr>{tbls_email_content}'

    success_txt, spce = f'<h2>{_success_execution}</h2>', '<br>&nbsp;&nbsp;&nbsp;&nbsp;- '
    header = f'{success_txt}{spce}<b>Pipeline: </b>{pipeline_name}{spce}<b>{st_dt_lbl}</b> {ppline_strt_dt}{spce}<b>{end_dt_lbl}</b> {ppline_end_dt}{spce}<b>{start_time_lbl}</b> {start_time}'
    tbls_email_content = f'<span style="font-size: 13px;">{header}</span><p>{row_sep}</p><table style="font-size: 13px;" border="1">{tbls_email_content}</table>'
    subject = f'{sbjct_prfix} e2e-Data Pipeline ({pipeline_name}) {sbjct_sffix}'         

    receiver_email, receiver_name = env('PPLINE_RESULT_EMAIL'), env('PPLINE_RESULT_EMAIL_RCVR')
    SimpleAPIMailer.send_email(receiver_email, receiver_name, tbls_email_content, subject)



def get_join_query_from_tables(tables, db_path, db_file, schema, fields):
    
    query = f'''
        WITH target_tables AS ( SELECT * FROM ( VALUES {tables}) AS t(table_name)),
        base_table AS ( SELECT m.table_name FROM {db_file}.dwhperformance_meta.fk_map m
            JOIN target_tables t ON m.table_name = t.table_name
            GROUP BY m.table_name ORDER BY COUNT(*) DESC LIMIT 1
        ),
        raw_joins AS (
            SELECT DISTINCT
                b.table_name AS base_table_name,
                CASE 
                    WHEN m.table_name = b.table_name THEN m.ref_table
                    ELSE m.table_name
                END AS join_target_table,
                CASE 
                    WHEN m.table_name = b.table_name 
                    THEN 
                        CASE 
                            WHEN ROW_NUMBER() OVER (PARTITION BY m.ref_table ORDER BY m.fk_col) = 1 
                            THEN 'INNER JOIN {schema}.' || m.ref_table || ' AS ' || m.ref_table || ' ON {schema}.' || m.table_name || '.' || m.fk_col || ' = ' || m.ref_table || '.' || m.ref_col
                            ELSE 'INNER JOIN {schema}.' || m.ref_table || ' AS ' || m.ref_table || '__via__' || m.fk_col || ' ON {schema}.' || m.table_name || '.' || m.fk_col || ' = ' || m.ref_table || '__via__' || m.fk_col || '.' || m.ref_col
                        END
                    ELSE 
                        CASE 
                            WHEN ROW_NUMBER() OVER (PARTITION BY m.table_name ORDER BY m.fk_col) = 1 
                            THEN 'INNER JOIN {schema}.' || m.table_name || ' AS ' || m.table_name || ' ON {schema}.' || b.table_name || '.' || m.ref_col || ' = ' || m.table_name || '.' || m.fk_col
                            ELSE 'INNER JOIN {schema}.' || m.table_name || ' AS ' || m.table_name || '__via__' || m.fk_col || ' ON {schema}.' || b.table_name || '.' || m.ref_col || ' = ' || m.table_name || '__via__' || m.fk_col || '.' || m.fk_col
                        END
                END AS join_string
            FROM {db_file}.dwhperformance_meta.fk_map m
            CROSS JOIN base_table b
            JOIN target_tables t1 ON m.table_name = t1.table_name
            JOIN target_tables t2 ON m.ref_table = t2.table_name
            WHERE m.table_name = b.table_name OR m.ref_table = b.table_name
        )
        SELECT 
            'SELECT {fields} FROM {schema}.' || b.table_name || ' ' || STRING_AGG(j.join_string, ' ') AS generated_sql FROM base_table b
        JOIN raw_joins j ON b.table_name = j.base_table_name GROUP BY b.table_name;
    '''
    result = None
    with duckdb.connect(db_path) as con:
        result = con.query(query).fetchall()
    return result[0][0] if result else None



def get_table_columns(db_path, db_name):
    con = duckdb.connect(db_path)
    query = f'''
        WITH unique_columns AS (
            SELECT 
                table_name,
                column_name,
                data_type,
                ordinal_position,
                COLUMN_COMMENT
            FROM information_schema.columns 
            WHERE table_catalog = '{db_name}' 
            AND table_schema NOT IN ('dwhperformance_meta')
            QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY ordinal_position) = 1
        )
        SELECT 
            json_group_object(table_name, columns_array) as final_json
        FROM (
            SELECT 
                table_name, 
                json_group_array(
                    json_object(
                        'original_column_name', column_name,
                        'column_name', column_name,
                        'data_type', data_type,
                        'semantic_concept', column_name,
                        'confidence_score', ordinal_position,
                        'description', COLUMN_COMMENT
                    )
                ) as columns_array
            FROM unique_columns
            GROUP BY table_name
        ) AS table_subquery;
    '''
    result = con.query(query).fetchall()
    return result
    ...