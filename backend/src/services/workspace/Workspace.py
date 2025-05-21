import subprocess
from pathlib import Path
import uuid
import os
import duckdb
from utils.duckdb_util import DuckdbUtil
import re
from tabulate import tabulate

pattern = r'^use.*$'

class Workspace:
    
    editorLanguages = {
        'PYTHON': 'python-lang',
        'SQL': 'sql-lang',
    }

    duckdb_open_connections = {}

    def __init__(self):
        self.sql_run_last_ppline_file = None

    @staticmethod
    def connect_to_duckdb(database):

        if database in Workspace.duckdb_open_connections:
            return {
                'connection': Workspace.duckdb_open_connections[database]
            }

        try:
            folder = Workspace.get_duckdb_path_on_ppline()
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
    def execute_sql_query(code):
        instance = Workspace()
        queries = instance.parse_sql_query(code)
        queries = [Workspace.connect_and_run_sql_code(q['dbase'],q['table']) for q in queries if q is not None]
        
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
    def connect_and_run_sql_code(database, query_string):
        
        try:
            con = Workspace.connect_to_duckdb(f'{database}.duckdb')['connection']
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
    def list_duck_dbs():

        skip_tables = ['_dlt_loads','_dlt_pipeline_state','_dlt_pipeline_state','_dlt_version']

        folder = Workspace.get_duckdb_path_on_ppline()
        file_list = os.listdir(folder)
        result, tables = {}, None

        for _file in file_list:

            if _file.endswith('.duckdb') or _file.endswith('.db'):
                if _file not in result:
                    result[_file] = {}

                tables = DuckdbUtil.get_tables(f'{folder}/{_file}').fetchall()
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

