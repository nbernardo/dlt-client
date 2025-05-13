import subprocess
from pathlib import Path
import uuid

class Workspace:
    
    @staticmethod
    def run_code(payload):
        file_path = f'{Workspace.get_code_path()}/code_{uuid.uuid4()}.py'
        with open(file_path, 'x+') as _file:
            _file.write(payload)

        try:
            result = subprocess.run(['python', file_path],
                                check=True,
                                capture_output=True,
                                text=True)
            
            final_result = result.stdout if result.stderr == '' or result.stderr == 0 else result.stderr
            
        except subprocess.CalledProcessError as err:
            final_result = err.stderr
        print('DIDA FROM SERVICE')
        return final_result
        

    @staticmethod
    def get_code_path():
        root_dir = str(Path(__file__).parent).replace('src/services/workspace', '')
        return f'{root_dir}/destinations/code'


    @staticmethod
    def get_duckdb_path_on_ppline():
        root_dir = str(Path(__file__).parent)
        return f'{root_dir}/duckdb'

