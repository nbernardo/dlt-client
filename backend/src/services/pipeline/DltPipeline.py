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
from utils.SQLDatabase import SQLDatabase
import uuid
from datetime import datetime
import time
from utils.code_node_util import valid_imports, FORBIDDEN_CALLS, FORBIDDEN_CALLS_REGEX


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

        if int(data['bucketFileSource']) == 2:
            template = DltPipeline.get_s3_no_auth_template()

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
        try:
            check_invalid_code(data)
        except RuntimeError as err:
            return { 'error': True, 'status': False, 'message': str(err) }
        
        is_sql_destination = len(context.sql_destinations) > 0
        is_code_to_code_ppline = context.code_source and context.is_code_destination
        does_have_metadata = is_sql_destination == True or is_code_to_code_ppline == True

        filename_suffixe = '|withmetadata|' if does_have_metadata else ''
        if(filename_suffixe == ''):
            filename_suffixe = '|toschedule|' if context.pipeline_action == 'onlysave' else ''
        ppline_file = f'{file_path}/{file_name}{filename_suffixe}.py'
        file_open_flag = 'x+'
        
        self.curr_file = ppline_file
        if context.action_type == 'UPDATE':
            ppline_file = DltPipeline.create_new_pipline_version(file_name, file_path, data)
        else:
            # Create python file with pipeline code
            with open(ppline_file, file_open_flag, encoding='utf-8') as file:                    
                file.write(data)

        if context.pipeline_action == 'onlysave':
            context.emit_ppsuccess()
            return { 'status': True, 'message': 'Pipeline created successfully' }

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
        #  ppline update, otherwise flag = True will print the log in any scenario
        #  flag = context.transformation is not None or context.action_type == 'UPDATE'
        flag = True

        if(flag):
            while True:
                line = result.stdout.readline()
                time.sleep(0.1)

                if not line:
                    break
                line = line.strip()
                is_transformation_step = (line.endswith('Transformation')\
                                           and line.startswith('dynamic-_cmp'))
                
                if (line == 'RUN_SUCCESSFULLY'):
                    context.emit_ppsuccess()
                    pipeline_exception = False

                else:
                    if(is_transformation_step and pipeline_exception == False):

                        component_ui_id = line
                        Transformation(None, context, component_ui_id).notify_completion_to_ui()
                        
                    elif(line.startswith('RUNTIME_ERROR:') or pipeline_exception == True):

                        pipeline_exception = True
                        message = line.startswith('RUNTIME_ERROR:')
                        context.emit_ppline_trace(line.replace('RUNTIME_ERROR:',''), error=True)

                    elif(line.startswith('RUNTIME_WARNING:') or pipeline_exception == True):
                        context.emit_ppline_trace(line.replace('RUNTIME_WARNING:',''), warn=True)
                    else:
                        context.emit_ppline_trace(line)
            
        result.kill()

        if pipeline_exception == True:
            return { 'status': False, 'message': 'Runtime Pipeline error, check the logs for details' }

        message, status = 'Pipeline run terminated successfully', True
        
        error_messages, warning_status = None, False
        if result.returncode != 0 and not(context.action_type == 'UPDATE' and result.returncode == 2):
            error_messages = result.stderr.read().split('\n')
            if(str(error_messages).__contains__('[WARNING]')):
                context.emit_ppline_trace(error_messages, warn=True)
                warning_status = True
            else:
                message, status = '\n'.join(error_messages[1:]), False
                context.emit_ppline_trace(message, error=True)

        context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')
        
        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout.read())
        print("Standard Error:", message if error_messages != None else None)

        if (error_messages != None or result.returncode == 1) and warning_status == False:
            status = False
        else:
            status = status if len(result.stderr.read()) > 0 else True

        return { 'status': status, 'message': message }


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
    def get_s3_no_auth_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/simple_s3_anon_login.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt

    @staticmethod
    def get_api_templete():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/api.txt'

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
    def get_sql_db_template(tamplate_name = None):
        """
        This is template handling method
        """
        tplt = ''
        tplt_file = tamplate_name if tamplate_name != None else 'sql_db.txt'
        file_name = f'{template_dir}/{tplt_file}'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt    
    

    @staticmethod
    def get_mssql_db_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/sql_server.txt'

        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()

        return tplt    


    @staticmethod
    def get_dlt_code_template():
        """
        This is template handling method
        """
        tplt = ''
        file_name = f'{template_dir}/dlt_code.txt'

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
        if(type(self.curr_file) == str):
            os.remove(self.curr_file)
        ...


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

        if not(os.path.exists(ppline_file)):
            ppline_file = f'{destinations_dir}/{file_path}|toschedule|.py'
            if not(os.path.exists(ppline_file)):
                ppline_file = f'{destinations_dir}/{file_path}|withmetadata|.py'

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
                time.sleep(0.1)
                line = result.stdout.readline()
                if not line: 
                    break
                line = line.strip()
                if (line == 'RUN_SUCCESSFULLY'):
                    context.emit_ppsuccess()
                    pipeline_exception = False   
                
                else:
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

                             
            #if result.returncode == 0 and context is not None and pipeline_exception == False:
            #    context.emit_ppsuccess()

            result.kill()
            
            if pipeline_exception == True:
                message = 'Runtime Pipeline error, check the logs for details'
                context.emit_ppline_job_trace(message, error=True)
            else:
                if(line.__contains__('Pipeline run terminated successfully')):
                    if(has_ppline_job('end',job_execution_id)):
                        pass
                context.emit_ppline_job_trace('Pipeline run terminated successfully')
            
            error_messages, status = None, True
            if result.returncode != 0:
                err = str(result.stderr.read())
                print('THE ERROR IS ABOUT: ', err)
                if(err.__contains__('Could not set lock on file')):
                    pass

                error_messages = err.split('\n')
                if(str(error_messages).__contains__('[WARNING]')):
                    context.emit_ppline_trace(error_messages, warn=True)
                else:
                    message = '\n'.join(error_messages[1:])
                    context.emit_ppline_job_trace(message, error=True)
                    status = False

            if(status):
                context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')

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
        time = datetime.now()
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET last_run='{time}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)


    @staticmethod
    def read_pipeline(file_path, namespace):

        try:
            code = ''
            with open(file_path, 'r') as file:
                code = file.read()
                pipeline_code = json.loads(code)

            node_list = pipeline_code['content']['Home']['data']
            database_obj = { id: node for id, node in node_list.items() if node['name'] == 'SqlDBComponent' }

            datasource_details = None
            if(len(database_obj.keys()) > 0):

                node = list(database_obj.values())[0]
                connection_name = node['data']['connectionName']
                datasource_details = SQLDatabase.get_tables_list(namespace, connection_name)
            
            if not(not(datasource_details)):
                del datasource_details['details']
                
            return pipeline_code, datasource_details
        
        except Exception as err:
            return {}, {}
        
    
    @staticmethod
    def get_sqldb_transformation_preview(namespace, dbengine, connection_name, script):
        result = run_transform_preview(namespace, dbengine, connection_name, script)
        return result        
    

    @staticmethod
    def get_file_data_transformation_preview(script):
        result = run_transform_preview(None, None, None, script)
        return result


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


import ast

def check_unsafe_statements(code):
    """
    Raises ValueError if code contains disallowed statements or function calls
    """
    tree = ast.parse(code, mode="exec")
    valid_attrs = ['scan_csv','scan_parquet','scan_ndjson','with_columns','filter','collect','append','all','contains','lit','limit']

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ValueError("Import statements are not allowed")

        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_CALLS:
                raise ValueError(f"Use of function '{node.func.id}' is not allowed")

            #if isinstance(node.func, ast.Attribute) and not(node.func.attr in valid_attrs):
            #    raise ValueError("Attribute calls are not allowed")


def check_invalid_code(code):
    """
    Raises ValueError if code contains disallowed statements or function calls
    """
    
    code_lines = str(code).split('\n')

    for line in code_lines:

        if FORBIDDEN_CALLS_REGEX.search(line):
            raise RuntimeError('Invalid code provided which might cause security breach')

        line_of_code = line.strip()
        is_from_import = line_of_code.startswith('from ')\
              and line.strip().__contains__(' import ')
        
        is_import = line_of_code.strip().startswith('import ')

        if(is_from_import or is_import):
            if not(line_of_code in valid_imports):
                raise RuntimeError('Invalid code provided which might cause security breach')



from utils.SQLServerUtil import column_type_conversion
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy import inspect
import polars as pl

def run_transform_preview(namespace, dbengine, connection_name, script):

    try:
        check_unsafe_statements(script)        
    except Exception as err:
        return { 'error': True, 'result': { 'msg': str(err), 'code': None } }

    try:
        engine, inspector = None, None
        inner_env = { 'pl': pl }

        if(namespace != None):
            engine = SQLDatabase.get_connnection(namespace,dbengine,connection_name)
            inspector = inspect(engine)
        
            inner_env = {
                'engine': engine, 'pl': pl, 'inspector': inspector,
                'column_type_conversion': column_type_conversion,
            }

        compile(script, '<transformation_task>', 'exec')

    except NoInspectionAvailable as err:
        error = 'Error while trying to connect to database'
        print(f'{error}: ', str(err))
        return { 'error': True, 'result': { 'msg': error, 'code': None } }        

    except SyntaxError as err:
        print('Error while running pipeline transformation preview: ', err.text)
        return { 'error': True, 'result': { 'msg': 'Syntax error', 'code': err.text } }

    except Exception as err:
        print('Error while running pipeline transformation preview: ');
        return { 'error': True, 'result': str(err) }

    try:
        exec(script, {}, inner_env)
    except Exception as err:
        print('Error while running pipeline transformation preview: ')
        print(err)
        return { 'error': True, 'result': { 'msg': str(err), 'code': None } }

    return { 'error': False, 'result': inner_env['results'] if 'results' in inner_env else None }