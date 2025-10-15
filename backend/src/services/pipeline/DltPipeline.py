import os
import subprocess
import duckdb
import json
from controller.RequestContext import RequestContext
from pathlib import Path
from typing import Dict
from node_mapper.Transformation import Transformation
from utils.FileVersionManager import FileVersionManager
from utils.duckdb_util import DuckdbUtil
from utils.cache_util import DuckDBCache
import uuid
from datetime import datetime


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
        This is the pipeline new version creation
        """
        ppline_file = f'{file_path}/{file_name}.py'
        file_open_flag = 'x+'
        
        if context.action_type == 'UPDATE':
            ppline_file = DltPipeline.create_new_pipline_version(file_name, file_path, data)
        else:
            self.curr_file = ppline_file
            # Create python file with pipeline code
            with open(ppline_file, file_open_flag, encoding='utf-8') as file:
                file.write(data)

        # Run pipeline generater above by passing the python file
        result = subprocess.Popen(['python', ppline_file],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    bufsize=1
                                )
        pipeline_exception = False

        # TODO: If needed, flag can be assigned with proper logic so UI logs will only came in 
        #  specific situation like will only print if the ppline has transformation or if it's
        #  ppline update, otherwise flag = True will print in log in any scenario
        #  flag = context.transformation is not None or context.action_type == 'UPDATE'
        flag = True

        if(flag):
            while True:
                line = result.stdout.readline()
                if not line:
                    break
                line = line.strip()
                is_transformation_step = (line.endswith('Transformation')\
                                           and line.startswith('dynamic-_cmp'))
                
                if(is_transformation_step and pipeline_exception == False):
                    component_ui_id = line
                    Transformation(None, context, component_ui_id).notify_completion_to_ui()
                elif(line.startswith('RUNTIME_ERROR:') or pipeline_exception == True):
                    pipeline_exception = True
                    message = line.startswith('RUNTIME_ERROR:')
                    context.emit_ppline_trace(line.replace('RUNTIME_ERROR:',''), error=True)
                else:
                    context.emit_ppline_trace(line)

        result.wait()
           
        if result.returncode == 0 and context is not None and pipeline_exception == False:
            context.emit_ppsuccess()
        if pipeline_exception == True:
            return { 'status': False, 'message': 'Runtime Pipeline error, check the logs for details' }

        message, status = 'Pipeline run terminated successfully', True
        
        error_messages = None
        if result.returncode != 0:
            error_messages = result.stderr.read().split('\n')
            message, status = '\n'.join(error_messages[1:]), False
            context.emit_ppline_trace(message, error=True)
        
        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout.read())
        print("Standard Error:", message if error_messages != None else None)

        result.kill()

        return { 'status': status if len(result.stderr.read()) > 0 else True, 'message': message }


    def save_diagram(self, diagrm_path, file_name, content, pipeline_lbl, is_update = None, write_log = True):
        
        # Create the pipeline Diagram code
        pipeline_code = { 'content': content, 'pipeline_lbl': pipeline_lbl }
        if is_update == True:
            file_manager = FileVersionManager(diagrm_path)
            file_manager.save_version(
                f'{diagrm_path}/{file_name}.json', json.dumps(pipeline_code), 'updating transformation', write_log
            )
        else:
            diagrm_file, file_open_flag = f'{diagrm_path}/{file_name}.json', 'x+'
            with open(diagrm_file, file_open_flag) as file:
                file.write(json.dumps(pipeline_code))




    def update(self, file_path, file_name, data, context: RequestContext = None) -> Dict[str,str]:
        """
        This is the pipeline update and pipeline code
        """
        ppline_file, _ = f'{file_path}/{file_name}', 'w+' 
        self.curr_file = ppline_file

        file_manager = FileVersionManager(file_path)
        file_manager.save_version(file_name)
 
 
        # Run pipeline generater above by passing the python file
        result = subprocess.run(['python', ppline_file],check=True,
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
        Update the pipeline and file content It uses the create 
        method which will update since it exists already
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
    def get_transform_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple_transform_field.txt'

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


    @staticmethod
    def get_template_from_existin_ppline(ppline_path):
        """
        This is template handling method that retrieves from exsiting pipeline
        """
        tplt = ''

        with open(f'{ppline_path}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt
    
    @staticmethod
    def create_new_pipline_version(file_name, file_path, data):
        file_manager = FileVersionManager(file_path)
        file_manager.save_version(f'{file_path}/{file_name}.py', data, 'altering transformation')
        new_file_name_version = file_manager.get_latest_version(f'{file_name}.py')

        return new_file_name_version
    

    processed_job = { 'start': {}, 'end': {} }
    @staticmethod
    def run_pipeline_job(file_path, namespace):
        ppline_file = f'{destinations_dir}/{file_path}.py'
        db_root_path = destinations_dir.replace('pipeline','duckdb')
        # DB Lock in the pplication level
        DuckDBCache.set(f'{db_root_path}/{file_path}.duckdb','lock')

        socket_id = DuckdbUtil.get_socket_id(namespace)
        context = RequestContext(None, socket_id)
        job_execution_id = uuid.uuid4()

        try:
            
            DuckdbUtil.check_pipline_db(f'{db_root_path}/{file_path}.duckdb')
            print('####### WILL RUN JOB FOR '+file_path)

            # Run pipeline generater above by passing the python file
            result = subprocess.Popen(['python', ppline_file],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True,
                                        bufsize=1)
            pipeline_exception = False

            while True:
                line = result.stdout.readline()
                if not line: 
                    break
                line = line.strip()
            
                if(line.startswith('RUNTIME_ERROR:') or pipeline_exception == True):
                    pipeline_exception = True
                    message = line.startswith('RUNTIME_ERROR:')
                    context.emit_ppline_job_trace(line.replace('RUNTIME_ERROR:',''), error=True)
                else:
                    if(type(line) == str):
                        if(line.__contains__('Files/Bucket loaded')):
                            if(has_ppline_job('start',job_execution_id)):
                                pass
                    context.emit_ppline_job_trace(line)

            result.wait()
            
            #if result.returncode == 0 and context is not None and pipeline_exception == False:
            #    context.emit_ppsuccess()

            if pipeline_exception == True:
                message = 'Runtime Pipeline error, check the logs for details'
                context.emit_ppline_job_trace(message, error=True)
            else:
                if(line.__contains__('Pipeline run terminated successfully')):
                    if(has_ppline_job('end',job_execution_id)):
                        pass
                context.emit_ppline_job_trace('Pipeline run terminated successfully')
            
            error_messages = None
            if result.returncode != 0:
                err = result.stderr.read()
                if(err.index('Could not set lock on file') > 0):
                    pass
                error_messages = err.split('\n')
                message = '\n'.join(error_messages[1:])
                context.emit_ppline_job_trace(message, error=True)
            
            result.kill()
            clear_job_transaction_id(job_execution_id)

            dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ppline_name = str(file_path).replace(f'{namespace}/','')
            DltPipeline.update_pipline_runtime(namespace,ppline_name,dt)

            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{file_path}.duckdb')
        
        except Exception as err:
            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{file_path}.duckdb')
            message = f'Error while running job for {file_path.split('/')[1]} pipeline'
            context.emit_ppline_job_trace(message,error=True)
            context.emit_ppline_job_trace(err.with_traceback,error=True)


    @staticmethod
    def update_pipline_runtime(namespace, ppline, time):
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET last_run='{time}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)


    @staticmethod
    def get_pipline_runtime(namespace, ppline):
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET last_run='{time}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)


def has_ppline_job(evt, job_transaction_id):

    if(DltPipeline.processed_job[evt].get(job_transaction_id,None) == None):
        DltPipeline.processed_job['end'][job_transaction_id] = True
        return False
    else:
        return True


def clear_job_transaction_id(job_transaction_id):
    if(job_transaction_id in DltPipeline.processed_job['start']):
        del DltPipeline.processed_job['start'][job_transaction_id]
    
    if(job_transaction_id in DltPipeline.processed_job['end']):
        del DltPipeline.processed_job['end'][job_transaction_id]