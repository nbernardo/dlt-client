import subprocess
from pathlib import Path
import uuid
import os
import duckdb

class Workspace:
    
    editorLanguages = {
        'PYTHON': 'python-lang',
        'SQL': 'sql-lang',
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
    def run_sql_code(query_string):

        try:
            con = duckdb.connect("/Users/nakassonybernardo/projects/dlt-client/backend/destinations/duckdb/pipeline_name.duckdb")
            con.execute(query_string)
            return con.fetchall()
        except Exception as err:
            return str(err)
        

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

