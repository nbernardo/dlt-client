import os
import subprocess
import duckdb
import json
from os import getenv as env
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
from utils.code_node_util import valid_imports, FORBIDDEN_CALLS, FORBIDDEN_CALLS_REGEX, FORBIDDEN_DUNDER_REGEX
import schedule
import logging
from utils.logging.pipeline_logger_config import handle_pipeline_log
import re
from utils.metastore.meta_storage import MetaStore
from utils.metastore.PipelineMedatata import PipelineMedatata
from utils.metastore.PipelineCheckpoint import PipelineCheckpoint
from utils.pipeline.Enums import Checkpoint
from services.email.SimpleAPIMailer import SimpleAPIMailer
from internationalization.email import labels
import asyncio


root_dir = str(Path(__file__).parent.parent.parent)
destinations_dir = f'{str(Path(__file__).parent.parent.parent.parent)}/destinations/pipeline'
template_dir = f'{root_dir}/pipeline_templates'
SUCCESS_RUN_MESSAGE = 'Pipeline run terminated successfully'


def is_SAWarning(message):
    """ validate if there was SQLAlchemy warning (e.g. no rewriting existing table) """
    regex = r"SAWarning: Table ['\"](.+?)['\"] already"
    return re.search(regex, message, re.IGNORECASE)

class DltPipeline:
    """
    This is the class to create and handle pipelines
    """
    def __init__(self):
        self.curr_file = None
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def prepare_pipeline_env_vars():
        """
        Prepare environment variables for pipeline execution including Vault credentials
        Returns a copy of os.environ with Vault-related variables added
        """
        env_vars = os.environ.copy()
        if env('VAULT_ADDR'):
            env_vars['VAULT_ADDR'] = env('VAULT_ADDR')
        if env('VAULT_TOKEN'):
            env_vars['VAULT_TOKEN'] = env('VAULT_TOKEN')
        if env('HASHICORP_HOST'):
            env_vars['HASHICORP_HOST'] = env('HASHICORP_HOST')
        if env('HASHICORP_TOKEN'):
            env_vars['HASHICORP_TOKEN'] = env('HASHICORP_TOKEN')
        return env_vars


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
        try:
            check_invalid_code(data)
        except RuntimeError as err:
            return { 'error': True, 'status': False, 'message': str(err) }
        
        is_sql_destination = len(context.sql_destinations) > 0 if context else False
        is_code_to_code_ppline = (context.code_source and context.is_code_destination) if context else False
        does_have_metadata = is_sql_destination == True or is_code_to_code_ppline == True

        if not(does_have_metadata):
            does_have_metadata = True if (context and context.bucket_source and context.is_code_destination) else False

        filename_suffixe = ''
        if context.pipeline_metadata.existing_wd:
            filename_suffixe = '__withmetadata__'
        elif context and context.is_duck_destination != True:
            filename_suffixe = '__withmetadata__' if does_have_metadata or context.is_code_destination else ''
            if(filename_suffixe == ''):
                filename_suffixe = '__toschedule__' if context.pipeline_action == 'onlysave' else ''

        ppline_file = f'{file_path}/{file_name}{filename_suffixe}.py'
        file_open_flag = 'x+'
        
        self.curr_file = ppline_file
        
        if context and context.action_type == 'UPDATE':
            ppline_file = DltPipeline.create_new_pipline_version(file_name, file_path, data)
        else:
            # Create python file with pipeline code
            with open(ppline_file, file_open_flag, encoding='utf-8') as file:                    
                file.write(data)

        if context and context.pipeline_action == 'onlysave':
            context.emit_ppsuccess()
            MetaStore.persist_pipeline_metadata(
                context.transaction_namespace, context.pipeline_name, vars(context.pipeline_metadata), None, '', context.pipeline_metadata.pipline_plan_id
            )
            return { 'status': True, 'message': 'Pipeline created successfully' }
        
        PIPE = subprocess.PIPE

        # Run pipeline generater above by passing the python file
        result = subprocess.Popen(['python', ppline_file], stdout=PIPE, stdin=PIPE, stderr=PIPE, text=True, bufsize=1)
        start_time = str(datetime.now().timestamp())

        # TODO: If needed, flag can be assigned with proper logic so UI logs will only came in  specific situation like will 
        # only print if the ppline has transformation or if it's ppline update, otherwise flag = True will print the log in 
        # any scenario flag = context.transformation is not None or context.action_type == 'UPDATE'
        flag = True
        logger, refs = DltPipeline.get_pipeline_logger(context), {}

        result.stdin.write(start_time+'\n') # Writes the checkpoint pipeline start_time to the child/pipeline process 
        result.stdin.flush()

        if(flag):
            while True:
                line = result.stdout.readline()
                time.sleep(0.15)
                
                if line == '' or line.strip() == 'import pkg_resources' or line.strip().__contains__('import pkg_resources'): 
                    continue
                if DltPipeline._handle_pipeline_trace(line, refs, context, logger) == False or not line: 
                    break
            
        #result.kill() # Each process will be responsible to kill/exit ifself
        MetaStore.persist_pipeline_metadata(
            context.transaction_namespace, context.pipeline_name, vars(context.pipeline_metadata), refs.get('dataset_name'), refs.get('short_query'), 
            context.pipeline_metadata.pipline_plan_id
        )

        if refs.get('pipeline_exception') == True:
            handle_pipeline_log(f'PIPELINE FAILED: Pipeline {context.pipeline_name} with execution_id {context.pipeline_execution_id} failed', logger, True)
            return { 'status': False, 'message': 'Runtime Pipeline error, check the logs for details' }

        message, status = SUCCESS_RUN_MESSAGE, True
        
        error_messages, warning_status = None, False
        if (result.returncode != 0 and result.returncode != None)\
              and not (context and context.action_type == 'UPDATE' and result.returncode == 2):
            error_messages = result.stderr.read().split('\n')
            if str(error_messages).__contains__('[WARNING]'):
                if context:
                    context.emit_ppline_trace(error_messages, warn=True)
                    context.emit_ppsuccess()
                warning_status = True
            else:
                message, status = '\n'.join(error_messages[1:]), False
                if message.__contains__('import pkg_resources') or refs.get('pipeline_exception') == False:
                    message, status = SUCCESS_RUN_MESSAGE, True
                elif context and status == False:
                    context.emit_ppline_trace(message, error=True)
                    DltPipeline.send_fail_email(start_time, context)

        if context:
            context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')
            handle_pipeline_log(f'pipeline.success.conclusion', logger)
        
        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout.read())
        print("Standard Error:", message if error_messages != None else None)

        if(not(message.strip() == SUCCESS_RUN_MESSAGE)):
            if (error_messages != None or result.returncode == 1) and warning_status == False:
                status = True if message == SUCCESS_RUN_MESSAGE else False
            else:
                status = status if len(result.stderr.read()) > 0 else True

        if status == True: context.emit_ppsuccess()

        return { 'status': status, 'message': message }


    def save_diagram(self, diagrm_path, file_name, content, pipeline_lbl, is_update: bool|str = None, write_log = True):
        """
        Save pipeline diagram.
        """
        # Create the pipeline Diagram code
        pipeline_code = { 'content': content, 'pipeline_lbl': pipeline_lbl }
        if is_update == True:
            file_manager = FileVersionManager(diagrm_path)
            file_manager.save_version(
                f'{diagrm_path}/{file_name}.json', json.dumps(pipeline_code), 'updating transformation', write_log
            )
        else:
            from utils.pipeline.Enums import FileOperation
            diagrm_file = f'{diagrm_path}/{file_name}.json'

            if is_update == FileOperation.REPLACE:
                with open(diagrm_file,'w') as file:
                    file.write(json.dumps(pipeline_code))
            else:
                with open(diagrm_file, 'x+') as file:
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

        if result.returncode == 0:
            if context is not None:
                context.emit_ppsuccess()

        print("Return Code:", result.returncode)
        print("Standard Output:", result.stdout)
        print("Standard Error:", result.stderr)

        return {
            'status': True,
            'message': SUCCESS_RUN_MESSAGE
        }


    def update_ppline(self, file_path, file_name, data, context: RequestContext) -> Dict[str,str]:
        """
        Update the pipeline and file content It uses the create 
        method which will update since it exists already
        """
        self.update(file_path, file_name, data, context)

    @staticmethod
    def return_template(file_name, tplt):
        with open(f'{file_name}', 'r', encoding='utf-8') as file:
            tplt = file.read()
        return tplt

    @staticmethod
    def get_template():
        """ This is template handling method """
        return DltPipeline.return_template(f'{template_dir}/simple.txt', '')

    @staticmethod
    def get_s3_no_auth_template():
        """ This is template handling method """
        return DltPipeline.return_template(f'{template_dir}/simple_s3_anon_login.txt', '')


    @staticmethod
    def get_s3_auth_template():
        return DltPipeline.return_template(f'{template_dir}/simple_s3_auth.txt', '')

    @staticmethod
    def get_s3_auth_transform_template():
        return DltPipeline.return_template(f'{template_dir}/simple_s3_auth_transform_field.txt', '')

    @staticmethod
    def get_api_templete():
        return DltPipeline.return_template(f'{template_dir}/api.txt', '')

    @staticmethod
    def get_transform_template():
        return DltPipeline.return_template(f'{template_dir}/simple_transform_field.txt', '')
    
    @staticmethod
    def get_sql_db_template(tamplate_name = None):
        """ This is template handling method """
        tplt_file = tamplate_name if tamplate_name != None else 'sql_db.txt'
        return DltPipeline.return_template(f'{template_dir}/{tplt_file}', '')
    
    @staticmethod
    def get_mssql_db_template():
        """ This is template handling method """
        return DltPipeline.return_template(f'{template_dir}/sql_server.txt', '')

    @staticmethod
    def get_dlt_code_template():
        """ This is template handling method """
        return DltPipeline.return_template(f'{template_dir}/dlt_code.txt', '')


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
    

    def send_fail_email(start_time, context: RequestContext):
        start_dt, end_dt = datetime.fromtimestamp(float(start_time)), datetime.now()
        intl = labels[env('APP_LANG')]
        content = intl['PPLINE_FAIL_TXT'].replace('{pp_name}', context.pipeline_name).replace('{sdate}', start_dt).replace('{tstamp}', end_dt)
        [exec_fail_sbj_pfx, exec_fail_sbj_sfx] = [intl['PPLINE_FAIL_EXEC_SBJ_PFX'], intl['PPLINE_FAIL_EXEC_SBJ_SFX']]

        subject = f'{exec_fail_sbj_pfx} ({context.pipeline_name}) {exec_fail_sbj_sfx}'
        SimpleAPIMailer.send_email(env('PPLINE_RESULT_EMAIL'), env('PPLINE_RESULT_EMAIL_RCVR'), content, subject)



    def _handle_pipeline_trace(line, refs: dict, context: RequestContext, logger: logging.Logger):
        line = line.strip()
        
        if(line.startswith('DATA=__dlt__destination__datasetname__:')):
            refs['dataset_name'] = line.split(':')[1]
            return False # No error, just pipeline completion
        
        if(line.startswith('SHORT_QUERY=__e2e_short_query_:')):
            refs['short_query'] = line.split(':')[1]
            return True
        
        refs['transf_step'] = (line.endswith('Transformation') and line.startswith('dynamic-_cmp'))

        if (line == 'RUN_SUCCESSFULLY'):
            if context: context.emit_ppsuccess()
            refs['pipeline_exception'] = False if refs.get('pipeline_exception', False) == False else refs.get('pipeline_exception')
            return False # No error, just pipeline completion
        
        else:
            if(refs['transf_step'] and refs.get('pipeline_exception', False) == False):
                component_ui_id = line
                if context:
                    Transformation(None, context, component_ui_id).notify_completion_to_ui()

            elif(line.startswith('RUNTIME_WARNING:') or is_SAWarning(line)):
                refs['warning_message'] = line.replace('RUNTIME_WARNING:','')
                handle_pipeline_log(refs['warning_message'], logger, False, True)
                if context:
                    context.emit_ppline_trace(refs['warning_message'], warn=True)

            elif(line.startswith('RUNTIME_ERROR:') or line.startswith('ERROR:') or refs.get('pipeline_exception') == True):
                    refs['pipeline_exception'] = True
                    refs['error_message'] = line.replace('RUNTIME_ERROR:','').replace('ERROR:','')
                    handle_pipeline_log(refs['error_message'], logger, True)
                    context.emit_ppline_job_trace(refs['error_message'], error=True)
                    if line.startswith('ERROR:'): return False
            else:
                if(type(line) == str):
                    if(line.__contains__('Files/Bucket loaded')):
                        if(has_ppline_job('start',refs.get('job_execution_id'))): return True
                
                if context:
                    refs['ui_log'] = str(line).replace('[PIPELINE_LOG]:','').replace('[DLT]:','').replace(' |+| ','')
                    context.emit_ppline_job_trace(refs['ui_log'])
                    handle_pipeline_log('Scheduled-Job-log -> '+line, logger)
        return True


    def handle_job_final_state(context: RequestContext, pipeline_exception, line, job_execution_id, result, logger: logging.Logger, start_time):
        if pipeline_exception == True:
            message = f'Runtime Pipeline ({context.pipeline_name}) with execution_id {context.pipeline_execution_id} failed, check the logs for details'
            handle_pipeline_log(f'SCHEDULE PIPELINE FAILED: Pipeline {context.pipeline_name} with execution_id {context.pipeline_execution_id} failed', logger, True)
            context.emit_ppline_job_trace(message, error=True)
            DltPipeline.send_fail_email(start_time, context)


        else:
            if(line.__contains__(SUCCESS_RUN_MESSAGE)):
                if(has_ppline_job('end',job_execution_id)):
                    pass
            context.emit_ppline_job_trace(SUCCESS_RUN_MESSAGE)
        
        error_messages, status = None, True
        if result.returncode != 0:
            err = str(result.stderr.read())
            
            if(err.__contains__('Could not set lock on file')):
                pass

            error_messages = err.split('\n')
            if(str(error_messages).__contains__('[WARNING]')):
                context.emit_ppline_trace(error_messages, warn=True)
            else:
                message = '\n'.join(error_messages[1:])
                if message.__contains__('import pkg_resources'):
                    status = True
                if status == False:
                    context.emit_ppline_job_trace(message, error=True)
                    status = False

        if(status):
            context.emit_ppline_trace('PIPELINE COMPLETED SUCCESSFULLY')
            context.emit_ppsuccess()
            handle_pipeline_log(f'pipeline.success.conclusion', logger)

        clear_job_transaction_id(job_execution_id)


    @staticmethod
    def _get_scheduled_pipeline_file(ppline_file, file_path):
        if not(os.path.exists(ppline_file)):
            # Try new format (double underscore)
            ppline_file = f'{destinations_dir}/{file_path}__toschedule__.py'
            if not(os.path.exists(ppline_file)):
                ppline_file = f'{destinations_dir}/{file_path}__withmetadata__.py'
            # Try old format (single underscore) for backward compatibility
            if not(os.path.exists(ppline_file)):
                ppline_file = f'{destinations_dir}/{file_path}_toschedule_.py'
            if not(os.path.exists(ppline_file)):
                ppline_file = f'{destinations_dir}/{file_path}_withmetadata_.py'

        return ppline_file


    processed_job = { 'start': {}, 'end': {} }
    job_refs = {}

    @staticmethod
    def run_pipeline_job_sync(file_path, namespace, leade_pipeline = None):
        asyncio.run(DltPipeline.run_pipeline_job(file_path, namespace, leade_pipeline))


    @staticmethod
    async def run_pipeline_job(file_path, namespace, leade_pipeline = None, triggers = None):
        
        socket_id = DuckdbUtil.get_socket_id(namespace)
        context = RequestContext(None, socket_id)
        logger = DltPipeline.get_pipeline_logger(context)

        if(triggers != None):
            handle_pipeline_log(f'Trigger for {file_path} from {leade_pipeline}', logger, False)

        [ppline_file, pipeline] = [f'{destinations_dir}/{file_path}.py', file_path.replace(f'{namespace}/','')]
        [pipeline_metadata, db_file] = [PipelineMedatata.get_pipeline_metadata(pipeline, namespace), file_path]

        # In case it an integration pipeline, it'll use a 
        # shared storage (Duckdb) file set in the file_path
        if pipeline_metadata[7] != None:
            if pipeline_metadata[7].__contains__('.'):
                db_file = f'{namespace}/{pipeline_metadata[7].split('.')[1]}'
                file_path = db_file if triggers != None else file_path

        ppline_file = DltPipeline._get_scheduled_pipeline_file(ppline_file, file_path)
        # pipeline_using_storage = PipelineCheckpoint.check_dest_storge_usage(file_path, pipeline)
        # In case another pipeline is using the same destination storage (in case of Duckdb)
        #if(pipeline_using_storage != None):
        #    handle_pipeline_log(f'Delaying {pipeline} execution due to {pipeline_using_storage} being using the storage', logger)
        #    return PipelineCheckpoint.persist(pipeline, Checkpoint.DELAY, datetime.now().timestamp(), 'UNSET', file_path)

        db_root_path = destinations_dir.replace('pipeline','duckdb')
        # DB Lock in the pplication level
        if not(ppline_file.endswith('withmetadata__.py') and ppline_file.endswith('withmetadata__.py')):
            DuckDBCache.set(f'{db_root_path}/{db_file}.duckdb','lock')

        from utils.metastore.PipelineTrigger import PipelineTrigger

        refs, job_start_time = { 'job_execution_id': uuid.uuid4() }, None
        triggers = triggers if triggers != None else PipelineTrigger.find_all(namespace, pipeline)

        try:
            DuckdbUtil.check_pipline_db(f'{db_root_path}/{db_file}.duckdb')
            handle_pipeline_log(f'####### WILL RUN JOB FOR {file_path}', logger, False)

            # Run pipeline generater above by passing the python file
            # Pass environment variables including Vault credentials
            [env_vars, PIPE] = [DltPipeline.prepare_pipeline_env_vars(), asyncio.subprocess.PIPE]
            
            job_start_time = datetime.now().timestamp()
            proc = await asyncio.create_subprocess_exec('python', ppline_file, stdout=PIPE, stdin=PIPE, stderr=PIPE, env=env_vars)
            proc.stdin.write(str(job_start_time).encode() + b'\n') # Writes the checkpoint pipeline start_time to the child/pipeline process
            
            # Register pipeline run initiation and start time
            pipeline, storage_path, _ = PipelineCheckpoint.persist(pipeline, Checkpoint.INIT, job_start_time, Checkpoint.TIME_UNSET, db_file) 
            await proc.stdin.drain()

            while True:
                line = await proc.stdout.readline()

                if not line: break
                
                line = line.decode().strip()  
                if DltPipeline._handle_pipeline_trace(line, refs, context, logger) == False or not line: 
                    break

            #if proc.returncode == 0 and context is not None and pipeline_exception == False: context.emit_ppsuccess()
            # proc.kill()
            pipeline_exception = refs.get('pipeline_exception')
            DltPipeline.handle_job_final_state(context, pipeline_exception, line, refs.get('job_execution_id'), proc, logger, job_start_time)

            dt  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            [short_query, dataset_name] = [refs.get('short_query'), refs.get('dataset_name')]

            ppline_name = str(db_file).replace(f'{namespace}/','')
            DltPipeline.update_pipline_runtime(namespace,ppline_name,dt)

            delayed_pipeline = PipelineCheckpoint.check_delayed_pipeline(storage_path, pipeline)
            # Register pipeline run completion and end time, and add the found delayed_pipeline as the one taking the lock os storage
            PipelineCheckpoint.update(pipeline, delayed_pipeline, storage_path, job_start_time, Checkpoint.DONE)
            MetaStore.update_metadata(namespace, pipeline, dataset_name, short_query)

            if len(triggers) > 0:
                curr_trigger = triggers.pop(0)
                unity, wait_time = curr_trigger['time'], int(curr_trigger['unity'])
                target_pipeline = f'{namespace}/{curr_trigger['pipeline']}'

                if(unity == 'sec'): await asyncio.sleep(wait_time)    
                if(unity == 'min'): await asyncio.sleep(int(wait_time) * 60)

                await DltPipeline.run_pipeline_job(target_pipeline, namespace, pipeline, triggers)
                return

            #if(delayed_pipeline):
                # Pass the bastion to the delayed pipeline run. storage_path = db_file
                # return DltPipeline.run_pipeline_job(storage_path, namespace)
            
            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{db_file}.duckdb')
        
        except Exception as err:
            # DB Lock release in the pplication level
            DuckDBCache.remove(f'{db_root_path}/{db_file}.duckdb')
            message = f'Error while running job for {db_file.split('/')[1]} pipeline'
            
            context.emit_ppline_job_trace(message,error=True)
            context.emit_ppline_job_trace(err.with_traceback,error=True)
            handle_pipeline_log(refs.get('error_message'), logger, True)
            handle_pipeline_log(err.with_traceback, logger, True)
            DltPipeline.send_fail_email(job_start_time, context)


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
    def update_pipline_pause_status(namespace, ppline, is_paused):
        cnx = DuckdbUtil.get_workspace_db_instance()
        query = f"UPDATE ppline_schedule\
                    SET is_paused='{is_paused}'\
                    WHERE\
                        namespace='{namespace}'\
                        and ppline_name='{ppline}'"
        cnx.execute(query)

        if is_paused != 'paused':
            from services.workspace.Workspace import Workspace
            Workspace.schedule_pipeline_job(namespace, ppline, unpause=True)
        else:
            tag_name = f'{namespace}_{ppline}'
            if schedule.get_jobs(tag_name):
                schedule.clear(tag_name)

            if schedule.get_jobs(f'{tag_name}-tracinglog'):
                schedule.clear(f'{tag_name}-tracinglog')
                        


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
        from utils.metastore.meta_storage import MetaStore 
        try:
            code = ''
            with open(file_path, 'r') as file:
                code = file.read()
                pipeline_code = json.loads(code)

            datasource_details = None
            node_list = pipeline_code['content']['Home']['data']
            database_obj = { node['name']: node for _, node in node_list.items() if (node['name'] in ['Bucket','SqlDBComponent']) }

            if(len(database_obj.keys()) > 0): datasource_details = {}
            pipeline_name = file_path.split('/')[-1].replace('.json','')

            if('SqlDBComponent' in database_obj):

                node = database_obj['SqlDBComponent']
                connection_name = node['data']['connectionName']
                datasource_details['SqlDBComponent'] = {}
                datasource_details['SqlDBComponent']['sourceDb'] = SQLDatabase.get_tables_list(namespace, connection_name)
                datasource_details['SqlDBComponent']['metadata'] = MetaStore.get_pipeline_metadata(f'{namespace}_at_{pipeline_name}')
            
            elif ('Bucket' in database_obj):
                
                node = database_obj['Bucket']
                connection_name = node['data'].get('connectionName')
                if connection_name:
                    datasource_details['Bucket'] = MetaStore.get_pipeline_metadata(f'{namespace}_at_{pipeline_name}')
            
            #if not(not(datasource_details)):
            #    del datasource_details['details']
                
            return pipeline_code, datasource_details
        
        except Exception as err:
            return {}, {}
        

    @staticmethod
    def get_pipeline_source_destination_type(namespace):
        return MetaStore.get_pipeline_source_destination_type(namespace)

    
    @staticmethod
    def get_sqldb_transformation_preview(namespace, dbengine, connection_name, script):
        result = run_transform_preview(namespace, dbengine, connection_name, script)
        return result        
    

    @staticmethod
    def get_file_data_transformation_preview(script, connection_name, namespace):
        result = run_transform_preview(namespace, None, connection_name, script)
        return result
    

    @staticmethod
    def get_pipeline_logger(context: RequestContext) -> logging.Logger:

        pipeline_id = context.pipeline_name
        execution_id = context.pipeline_execution_id
        namespace = context.user
        return logging.getLogger(f'pipeline.{namespace}.{pipeline_id}.{execution_id}')


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

        if (FORBIDDEN_CALLS_REGEX.search(line) or FORBIDDEN_DUNDER_REGEX.search(line))\
            and not line.__contains__('__dlt__transaction_id:{'):
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
        bucket_credentials, bucket_name = None, None

        inner_env = { 'pl': pl }

        if(namespace != None):
            if connection_name == None: connection_name = ''
            if dbengine == None and connection_name.strip() != '':
                from utils.BucketConnector import get_bucket_credentials
                credentials = get_bucket_credentials(namespace,connection_name)
                bucket_credentials = {
                    "aws_access_key_id": credentials.get('access_key_id'),
                    "aws_secret_access_key": credentials.get('secret_access_key'),
                    "aws_region": credentials.get('region'),
                }
                bucket_name = f's3://{credentials.get('bucket_name')}/'

            else:
                if dbengine != None:
                    engine = SQLDatabase.get_connnection(namespace,dbengine,connection_name)
                    inspector = inspect(engine)
        
            inner_env = {
                'engine': engine, 'pl': pl, 'inspector': inspector, 
                'bucket_credentials': bucket_credentials, 'bucket_name': bucket_name,
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


def create_execution_id() -> str:
    """Generate a unique execution ID for pipeline runs."""
    return f"exec_{uuid.uuid4().hex[:12]}"
