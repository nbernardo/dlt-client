import subprocess
from pathlib import Path
import uuid
import os
import duckdb
import re
from utils.duckdb_util import DuckdbUtil
from tabulate import tabulate
from controller.pipeline import BasePipeline
from services.pipeline.DltPipeline import DltPipeline
import schedule
import asyncio

pattern = r'^use.*$'

class Workspace:
    
    editorLanguages = {
        'PYTHON': 'python-lang',
        'SQL': 'sql-lang',
    }

    duckdb_open_connections = {}
    duckdb_open_errors = {}

    def __init__(self):
        self.sql_run_last_ppline_file = None

    @staticmethod
    def connect_to_duckdb(database, user):
        if database in Workspace.duckdb_open_connections:
            return {
                'connection': Workspace.duckdb_open_connections[database]
            }

        try:
            folder = BasePipeline.folder+'/duckdb/'+user
            db_cnx = duckdb.connect(f'{folder}/{database}', read_only=True)
            Workspace.duckdb_open_connections[database] = db_cnx

        except Exception as e:
            return {
                'status': False,
                'message': f'Error on connecting to database {database}'               
            }

        return {
            'status': True,
            'message': f'Connection created successfully to {database}',
            'connection': Workspace.duckdb_open_connections[database]
        }


    @staticmethod
    def disconnect_duckdb(database, session):

        try:
            if database in Workspace.duckdb_open_connections:
                del Workspace.duckdb_open_connections[database]
            Workspace.duckdb_open_connections[database].close()


        except Exception as e:
            return {
                'status': False,
                'message': f'Error on disconnecting from database {database}'                
            }
        
        return {
            'status': True,
            'message': f'Disconnected successfully from {database}'
        }


    @staticmethod
    def run_python_code(payload):
        # create a fiile to hold the submitted code to be ran
        file_path = f'{Workspace.get_code_path()}/code_{uuid.uuid4()}.py'
        with open(file_path, 'x+') as _file:
            _file.write(payload)

        try:
            result = subprocess.run(['python', file_path],
                                check=True,
                                capture_output=True,
                                text=True)
            
            final_result = result.stdout \
                if result.stderr == '' or result.stderr == 0 \
                else result.stderr
            
        except subprocess.CalledProcessError as err:
            final_result = err.stderr

        if os.path.exists(file_path):
            #Remove the file after the code runs
            Workspace.remove_code_file(file_path)

        return final_result
        

    @staticmethod
    def execute_sql_query(code,user):
        instance = Workspace()
        queries = instance.parse_sql_query(code)
        queries = [Workspace.connect_and_run_sql_code(q['dbase'],q['table'],user) for q in queries if q is not None]
        
        return queries


    @staticmethod
    def run_sql_code(database, query_string):
        
        try:
            con = Workspace.duckdb_open_connections[database]
            con.execute(query_string)
            result = con.fetchall()
            return result
        except Exception as err:
            return str(err)


    @staticmethod
    def connect_and_run_sql_code(database, query_string, user):
        
        try:
            con = Workspace.connect_to_duckdb(f'{database}.duckdb',user)['connection']
            con.execute(query_string)
            result = con.fetchall()
            return tabulate(result, tablefmt="grid")
        
        except Exception as err:
            return str(err)
        

    def parse_sql_query(self, code):

        lines = code.replace('\n',' ').split(';')
        if(lines[-1:] == ''): lines.pop()

        def parse_use_stmt(stmt):
            self.sql_run_last_ppline_file = stmt.split('use ')[1].strip()
            return None
        
        def parse_sql(table):
            dbase = self.sql_run_last_ppline_file
            return { 'dbase': dbase, 'table': table }

        return [
            (parse_sql(line) if not re.match(pattern, line) else parse_use_stmt(line)) 
                for line in lines
        ]


    @staticmethod
    def list_duck_dbs(files_path, user):

        skip_tables = ['_dlt_loads','_dlt_pipeline_state','_dlt_pipeline_state','_dlt_version']

        #folder = Workspace.get_duckdb_path_on_ppline()
        file_list = os.listdir(files_path)
        result, tables = {}, None

        for _file in file_list:

            if _file.endswith('.duckdb') or _file.endswith('.db'):
                if _file not in result:
                    result[_file] = {}

                tables_list = Workspace.get_tables(f'{files_path}/{_file}',None, user)

                if 'error' in tables_list: 
                    continue
                
                tables = tables_list['tables'].fetchall()
                for t in tables:

                    if t[2] not in skip_tables:
                        ppline = t[0]
                        dbname = t[1]
                        table = t[2]
                        db_size = t[3]
                        col_count = t[4]

                        k = f'{ppline}-{dbname}-{table}'

                        result[_file][k] = { 
                            'ppline': ppline,
                            'dbname': dbname,
                            'table': table, 
                            'db_size': db_size,
                            'col_count': col_count
                        }

        return result
    
    @staticmethod
    def get_tables(database = None, where = None, user = None):
        try:
            cnx = duckdb.connect(f'{database}', read_only=True)
            cursor = cnx.cursor()
            where_clause = f'WHERE {where}' if where is not None else ''
            query = f"SELECT \
                        database_name, schema_name, table_name, \
                        estimated_size, column_count FROM \
                        duckdb_tables {where_clause}"
            print(f'Fetching DuckDb tables: {query}')
            
            return { 'tables': cursor.execute(query) }
        except duckdb.IOException as err:
            if not(user in Workspace.duckdb_open_errors):
                Workspace.duckdb_open_errors[user] = []

            Workspace.duckdb_open_errors[user].append(str(err))
            return { 'error': True, 'error_list': Workspace.duckdb_open_errors[user] }
    

    @staticmethod
    def update_socket_id(namespace, new_sock_id):
        try:
            if(DuckdbUtil.workspace_table_exists('socket_connection') == False):
                DuckdbUtil.create_socket_conection_table()

            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"INSERT INTO socket_connection (namespace,socket_id) VALUES ('{namespace}', '{new_sock_id}')\
                      ON CONFLICT (namespace) DO UPDATE SET namespace = EXCLUDED.namespace, socket_id = EXCLUDED.socket_id;"
            
            cursor.execute(query)

        except duckdb.IOException as err:
            print({ 'error': True, 'error_list': err })
    

    @staticmethod
    def update_namespace_alias(namespace, new_alias):
        try:
            table = 'namespace'
            if(DuckdbUtil.workspace_table_exists(table) == False):
                DuckdbUtil.create_namespace_alias_table()

            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"INSERT INTO {table} (namespace_id,namespaces_alias) VALUES ('{namespace}', '{new_alias}')\
                      ON CONFLICT (namespace_id) \
                      DO UPDATE SET namespace_id = EXCLUDED.namespace_id, namespaces_alias = EXCLUDED.namespaces_alias;"
            cursor.execute(query)

        except duckdb.IOException as err:
            print({ 'error': True, 'error_list': err })
    

    @staticmethod
    def create_ppline_schedule(ppline_name, schedule_settings, namespace, type, periodicity, time):
        try:
            table = 'ppline_schedule'
            if(DuckdbUtil.workspace_table_exists(table) == False):
                DuckdbUtil.create_ppline_schedule_table()

            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"INSERT INTO {table} (ppline_name,schedule_settings,namespace,type,periodicity,time)\
                      VALUES ('{ppline_name}', '{schedule_settings}', '{namespace}','{type}','{periodicity}','{time}')"
            cursor.execute(query)

        except duckdb.IOException as err:
            print({ 'error': True, 'error_list': err })
    

    @staticmethod
    def get_ppline_schedule(namespace = None):
        try:
            table = 'ppline_schedule'
            if(DuckdbUtil.workspace_table_exists(table) == False):
                DuckdbUtil.create_ppline_schedule_table()

            where = f"WHERE namespace = '{namespace}'" if namespace != None else ''
            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"SELECT ppline_name,schedule_settings,namespace,type,periodicity,time FROM {table} {where}"
            cursor.execute(query)
            return { 'data': cursor.fetchall(), 'error': False }

        except duckdb.IOException as err:
            print({ 'error': True, 'error_list': err })
            return { 'error': True, 'error_list': err }
    

    @staticmethod
    def alias_name_space(namespace, new_sock_id):
        try:
            if(DuckdbUtil.namespace_db_exists() == False):
                DuckdbUtil.create_namespace_alias()

            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"INSERT INTO namespace (namespace_id,alias) VALUES ('{namespace}', '{new_sock_id}')\
                      ON CONFLICT (namespace_id) DO UPDATE SET namespace_id = EXCLUDED.namespace_id, alias = EXCLUDED.alias;"
            cursor.execute(query)

        except duckdb.IOException as err:
            print({ 'error': True, 'error_list': err })

    schedule_jobs = dict()        

    @staticmethod
    def schedule_pipeline_job():
        result = Workspace.get_ppline_schedule()
        if result['error'] != True:
            schedules = result['data']
        
            for sched in schedules:
                ppline_name = sched[0]
                namespace = sched[2]
                file_path = f'{namespace}/{ppline_name}'

                if(Workspace.schedule_jobs.get(file_path,None) != True):
                    schedule.every(1).minutes.do(DltPipeline.run_pipeline_job, file_path, namespace)
                    schedule.every(20).seconds.do(lambda: print('Ola pessoal'))
                    print("Scheduling pipeline Job for "+file_path)
                    Workspace.schedule_jobs[file_path] = True

            while True:
                schedule.run_pending()
                asyncio.sleep(1)
        else:
            ...

        
    @staticmethod
    def get_code_path():
        root_dir = str(Path(__file__).parent).replace('src/services/workspace', '')
        return f'{root_dir}/destinations/code'


    @staticmethod
    def get_duckdb_path_on_ppline():
        root_dir = str(Path(__file__).parent).replace('src/services/workspace','destinations')
        return f'{root_dir}/duckdb'


    @staticmethod
    def get_ppline_path():
        root_dir = str(Path(__file__).parent).replace('src/services/workspace','destinations')
        return f'{root_dir}/pipeline'
    

    @staticmethod
    def remove_code_file(file):
        os.remove(file)

    

