import os
import subprocess
import duckdb
import json
from controller.RequestContext import RequestContext
from pathlib import Path
from typing import Dict

root_dir = str(Path(__file__).parent).replace('/src/services/pipeline', '')
destinations_dir = f'{root_dir}/destinations/pipeline'
template_dir = f'{root_dir}/src/pipeline_templates'


class DltPipeline:
    """
    This is the class to create and handle pipelines
    """
    def __init__(self):
        self.curr_file = None


    def create(self, data):
        """
        This is the pipeline creation
        """
        file_name = data['pipeline']

        file_path = f'{destinations_dir}/{file_name}.py'
        file_open_flag = 'x+'

        template = DltPipeline.get_template()

        if os.path.exists(file_path):
            return 'Pipeline exists already'
            
        with open(file_path, file_open_flag, encoding='utf-8') as file:
            for field in data.keys():
                template = template\
                    .replace(f'%{field}%', f'"{data[field]}"')

            file.write(template)

        result = subprocess.run(['python', file_path],
                                check=True,
                                capture_output=True,
                                text=True)

        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)


    def create_v1(self, file_path, file_name, data, context: RequestContext = None) -> Dict[str,str]:
        """
        This is the pipeline creation
        """
        pplint_file = f'{file_path}/{file_name}.py'
        file_open_flag = 'x+'
        self.curr_file = pplint_file

        if os.path.exists(pplint_file):
            message = 'Pipeline exists already'
            print(message)
            return { 'status': False, 'message': message }

        # Create python file with pipeline code
        with open(pplint_file, file_open_flag, encoding='utf-8') as file:
            file.write(data)

        # Run pipeline generater above by passing the python file
        result = subprocess.run(['python', pplint_file],
                                check=True,
                                capture_output=True,
                                text=True)

        if result.returncode == 0 and context is not None:
            context.emit_ppsuccess()

        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)

        return {
            'status': True,
            'message': 'Pipeline run terminated successfully'
        }


    def save_diagram(self, diagrm_path, file_name, conent):
        import json
        # Create python file with pipeline code
        diagrm_file, file_open_flag = f'{diagrm_path}/{file_name}.json', 'x+'
        with open(diagrm_file, file_open_flag) as file:
            file.write(json.dumps(conent))


    def update(self, file_path, file_name, data, context: RequestContext = None) -> Dict[str,str]:
        """
        This is the pipeline update and pipeline code
        """
        pplint_file, file_open_flag = f'{file_path}/{file_name}', 'w+' 
        self.curr_file = pplint_file

        # Create python file with pipeline code
        with open(pplint_file, file_open_flag, encoding='utf-8') as file:
            file.write(data.decode('utf-8'))

        
        # Run pipeline generater above by passing the python file
        result = subprocess.run(['python', pplint_file],check=True,
                                capture_output=True,text=True)

        if result.returncode == 0 and context is not None:
            context.emit_ppsuccess()

        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)


        return {
            'status': True,
            'message': 'Pipeline run terminated successfully'
        }


    def update_ppline(self, file_path, file_name, data, context: RequestContext) -> Dict[str,str]:
        """
        Update the pipeline and file content
        It uses the create method which will 
        update since it exists already
        """
        self.update(file_path, file_name, data, context)


    @staticmethod
    def get_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt
    
    @staticmethod
    def get_sql_db_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/sql_db.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt

    def save_instance(self, ppline_name, content):
        """
            This will save in the DB 
            the Pipeline created in the diagram (UI)
        """
        con = duckdb.connect("file.db")
        con.sql(
            "CREATE TABLE IF NOT EXISTS ppline_instances \
                (\
                    ppline_name, \
                    _content JSON, \
                    _timestamp TIMESTAMP\
                )\
            ")

        con.sql(
            f"INSERT INTO ppline_name, ppline_instances (_content) \
            VALUES \
            ('{ppline_name}','{json.dumps(content)}')")
        

    def revert_ppline(self):
        os.remove(self.curr_file)
