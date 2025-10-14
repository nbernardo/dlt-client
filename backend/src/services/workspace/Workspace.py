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
import time as timelib
from utils.cache_util import DuckDBCache

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

        if(not os.path.exists(files_path)):
            return {'no_data': True }
        
        skip_tables = ['_dlt_loads','_dlt_pipeline_state','_dlt_pipeline_state','_dlt_version']

        #folder = Workspace.get_duckdb_path_on_ppline()        
        file_list = os.listdir(files_path)
        result = { 'db_path': files_path } 
        tables = None
        k = None
        prev_key = None

        for _file in file_list:

            if _file.endswith('.duckdb') or _file.endswith('.db'):
                if _file not in result:
                    result[_file] = {}

                if DuckDBCache.get(f'{files_path}{_file}') != None:
                    result[_file][k] = { 
                        'ppline': str(_file).replace('.duckdb',''),
                        'dbname': None, 'table': [], 'db_size': None, 'col_count': 0, 'fields': [],
                        'flag': 'Pipeline tables in use by another process/Job'
                    }
                    continue

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
                        col_name = t[5]
                        col_type = t[6]
                        k = f'{ppline}-{dbname}-{table}'

                        if dbname.endswith('_staging'):
                            continue

                        if(k != prev_key):
                            result[_file][k] = { 
                                'ppline': ppline,
                                'dbname': dbname,
                                'table': table, 
                                'db_size': db_size,
                                'col_count': col_count,
                                'fields': [{ 'name': col_name, 'type': col_type }]
                            }

                        else:
                            result[_file][k]['fields'].append({ 'name': col_name, 'type': col_type })

                    prev_key = k
                k = None

        return result


    @staticmethod
    def list_duck_dbs_with_fields(files_path, user):

        skip_tables = ['_dlt_loads','_dlt_pipeline_state','_dlt_pipeline_state','_dlt_version']

        #folder = Workspace.get_duckdb_path_on_ppline()
        file_list = os.listdir(files_path)
        result, tables = {}, ''
        table_counter = 0
        current_pipeline = None
        current_schema = None
        
        for _file in file_list:

            if _file.endswith('.duckdb') or _file.endswith('.db'):
                if _file not in result:
                    result[_file] = {}

                ppline_file = f'{files_path}/{_file}'
                columns_list = Workspace.get_tables_and_field(f'{ppline_file}',None, user)

                if 'error' in columns_list: 
                    continue
                
                columns = columns_list['tables'].fetchall()
                for table_name, schema_name, column_name, data_type in columns:

                    if table_name not in skip_tables:

                        k = f'{ppline_file}-{schema_name}-{table_name}'
                        if k not in result:
                            table_counter = table_counter + 1
                            if table_counter > 1: 
                                tables += f'DB-File: {current_pipeline}'
                                tables += f'\nSchema: {current_schema}\n\n'

                            result[k] = True
                            tables += f"{table_counter}. Table: '{schema_name}.{table_name}'\n"\
                                      f'Columns:\n'
                        
                        current_pipeline = ppline_file
                        current_schema = schema_name
                        tables += f'- {column_name}\n'\
                
                tables += f'DB-File: {current_pipeline}'
                tables += f'\nSchema: {current_schema}'
               
        return tables


    @staticmethod
    def get_tables(database = None, where = None, user = None):
        try:
            cnx = DuckdbUtil.get_connection_for(f'{database}')
            cursor = cnx.cursor()
            where_clause = f'WHERE {where}' if where is not None else ''
            query = f"SELECT \
                        database_name, schema_name, table_name, \
                        estimated_size, column_count FROM \
                        duckdb_tables {where_clause}"

            new_query = f'SELECT DISTINCT\
                        t.database_name, t.schema_name, t.table_name,\
                        t.estimated_size, t.column_count,\
                        column_name, data_type \
                    FROM duckdb_tables t \
                    JOIN duckdb_columns c ON t.table_name = c.table_name \
                        {where_clause} \
                    ORDER BY t.table_name, column_name'
            
            print(f'Fetching DuckDb tables: {new_query}')
            
            return { 'tables': cursor.execute(new_query) }
        except duckdb.IOException as err:
            if not(user in Workspace.duckdb_open_errors):
                Workspace.duckdb_open_errors[user] = []

            Workspace.duckdb_open_errors[user].append(str(err))
            return { 'error': True, 'error_list': Workspace.duckdb_open_errors[user] }


    @staticmethod
    def run_sql_query(database = None, query = None):
        try:
            cnx = DuckdbUtil.get_connection_for(f'{database}')
            cursor = cnx.cursor()
            result = cursor.execute(query).fetchall()

            fields = query.lower().split('from')[0].split('select',1)[1]
            return { 'result': result, 'fields': fields }
        
        except duckdb.ParserException as err:

            print(f'Error while running query: {query}')
            print(str(err))
            return { 'error': True, 'result': str(err), 'code': 'err' }
        except duckdb.IOException as err:

            print(f'Error while accessing the DB: query: {query}')
            print(str(err))
            return { 'error': True, 'result': str(err), 'code': 'err' }
    
    
    @staticmethod
    def get_tables_and_field(database = None, where = None, user = None):
        try:
            cnx = DuckdbUtil.get_connection_for(f'{database}')
            cursor = cnx.cursor()
            where_clause = f'WHERE {where}' if where is not None else ''

            query = f"SELECT DISTINCT \
                    t.table_name, \
                    t.schema_name, \
                    column_name, \
                    data_type \
                FROM duckdb_tables t \
                JOIN duckdb_columns c ON t.table_name = c.table_name \
                {where_clause} \
                ORDER BY t.schema_name, t.table_name, column_name"
            
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

        field_names = [
            'id','ppline_name','schedule_settings','namespace',
            'type','periodicity','time', 'last_run'
        ]

        try:
            table = 'ppline_schedule'
            if(DuckdbUtil.workspace_table_exists(table) == False):
                DuckdbUtil.create_ppline_schedule_table()

            where = f"WHERE namespace = '{namespace}'" if namespace != None else ''
            cnx = DuckdbUtil.get_workspace_db_instance()
            cursor = cnx.cursor()
            query = f"SELECT {','.join(field_names)} FROM {table} {where}"
            cursor.execute(query)
            result = cursor.fetchall()
            final_data = []

            if(len(result)):
                for row in result:
                    row_to_json = dict(zip(field_names,row))
                    final_data.append(row_to_json)

            return { 'data': final_data, 'error': False }

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
                ppline_name = sched['ppline_name']
                namespace = sched['namespace']
                type = sched['type']
                periodicity = sched['periodicity']
                time = int(sched['time'])
                file_path = f'{namespace}/{ppline_name}'
                
                if(Workspace.schedule_jobs.get(file_path,None) != True):
                    if(type == 'min'):
                        schedule.every(time).minutes.do(DltPipeline.run_pipeline_job, file_path, namespace)
                    if(type == 'hour'):
                        schedule.every(time).hours.do(DltPipeline.run_pipeline_job, file_path, namespace)

                    print(f'Schedule a job for {file_path} to happen {periodicity} {time} {type}')
                    Workspace.schedule_jobs[file_path] = True

            while True:
                schedule.run_pending()
                timelib.sleep(1)
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

    

