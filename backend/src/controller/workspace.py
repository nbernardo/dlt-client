from os import getenv as env
from flask import Blueprint, request
from services.workspace.Workspace import Workspace
from services.workspace.SecretManager import SecretManager
from controller.pipeline import BasePipeline
from controller.file_upload import BaseUpload
from flask_cors import cross_origin
import threading
import requests
import time
from services.agents.DataQueryAIAssistent import DataQueryAIAssistent as Agent
from typing import List
import traceback
from flask import abort, send_file
from utils.cache_util import DuckDBCache
from datetime import datetime
from utils.SQLDatabase import SQLDatabase
from utils.workspace_util import handle_conversasion_turn_limit


workspace = Blueprint('workspace', __name__)
schedule_was_called = None
file_folder_map = { 'data':'dbs/files', 'pipeline': 'destinations/pipeline'}

@workspace.route('/workcpace/code/run/<user>', methods=['POST'])
def run_code(user):

    payload = request.get_json()
    code = payload['code']

    if payload['lang'] == Workspace.editorLanguages['PYTHON']:
        output = Workspace.run_python_code(code)

    if payload['lang'] == Workspace.editorLanguages['SQL']:
        output = Workspace.execute_sql_query(code, user)
    
    return { 'output': output, 'lang': payload['code'] }


@workspace.route('/workcpace/duckdb/list/<user>/<socket_id>', methods=['POST'])
def list_pipelines(user, socket_id):

    ppelines_path = BasePipeline.folder+'/pipeline/'+user+'/'
    duckdb_ppelines_path = BasePipeline.folder+'/duckdb/'+user+'/'

    ppelines = Workspace.list_pipeline_from_files(ppelines_path)
    duckdb_ppelines = Workspace.list_duckdb_dest_pipelines(duckdb_ppelines_path, user)
    
    errors_list = None

    if((user in Workspace.duckdb_open_errors)):
        
        if(len(Workspace.duckdb_open_errors[user]) > 0):
            errors_list = Workspace.duckdb_open_errors[user]
            del Workspace.duckdb_open_errors[user]

            return { 
                'error': True, 
                'message': 'Failed to connect some of the DuckDb, check the logs for details',
                'trace': errors_list
            }

    return { **ppelines, **duckdb_ppelines }


@workspace.route('/workcpace/duckdb/connect', methods=['POST'])
def connect_duckdb():
    payload = request.get_json()
    database = payload['database']
    session = payload['session']
    result = Workspace.connect_to_duckdb(database, session)
    return result


@workspace.route('/workcpace/sql_query', methods=['POST'])
def run_sql_query():
    payload = request.get_json()
    database = payload['database']
    query = payload['query']
    if DuckDBCache.get(database) != None:
        message = 'The database selected to query is in use by a pipeline JOB, pleas wait until it gets completed.'
        return { 'error': True, 'result': message }
    result = Workspace.run_sql_query(database, query)
    return result


@workspace.route('/workcpace/duckdb/disconnect', methods=['POST'])
def disconnect_duckdb():
    payload = request.get_json()
    database = payload['database']
    session = payload['session']
    result = Workspace.connect_to_duckdb(database, session)
    return result


@workspace.route('/workcpace/socket_id/<namespace>/<socket_id>', methods=['POST'])
def update_socket_id(namespace, socket_id):
    Workspace.update_socket_id(namespace, socket_id)
    return ''


@workspace.route('/workcpace/ppline/schedule/<namespace>', methods=['POST'])
def create_ppline_schedule(namespace):
    import json
    from services.pipeline.DltPipeline import DltPipeline
    import schedule

    ppline_name = None
    try:
        payload = request.get_json()
        settings = payload['settings']
        ppline_name = payload['ppline_name']
        type, periodicity, time = settings['type'], settings['periodicity'], settings['time']
        # socket_id = payload['socket_id']

        Workspace.create_ppline_schedule(
            ppline_name, json.dumps(settings), namespace, type, periodicity, time
        )
        file_path = f'{namespace}/{ppline_name}'
        Workspace.schedule_jobs[file_path] = True

        if(type == 'min'):
            schedule.every(int(time)).minutes.do(DltPipeline.run_pipeline_job, file_path, namespace)
        if(type == 'hour'):
            schedule.every(int(time)).hours.do(DltPipeline.run_pipeline_job, file_path, namespace)

        schedule.every(20).seconds.do(lambda: print(f'Preparing to run job for {file_path} pipeline'))
        print(f'Schedule a job for {file_path} to happen {periodicity} {time} {type}')
        schedule.run_pending()

    except Exception as error:
        print(f'Error while trying to schedule {ppline_name} pipeline')
        print(error)
        return 'failed'
    return 'success'


@workspace.route('/workcpace/ppline/schedule/<namespace>', methods=['GET'])
def get_ppline_schedule(namespace):
    try:
        return Workspace.get_ppline_schedule(namespace)
    except Exception as error:
        print(f'Error while trying to connect with AI agent')
        print(error)
        return 'failed'


@workspace.route('/workcpace/init/<namespace>', methods=['GET'])
def get_initial_data(namespace):

    today_date = datetime.now().strftime('%d/%m/%Y')
    k = f'{today_date}/{namespace}'
    user_message_count_limit = env('CONVERSATION_TURN_LIMIT')
    ai_agent_namespace_details = {
        'conversation_count': DuckDBCache.get(k),
        'user_message_count_limit': user_message_count_limit
    }

    try:
        total_pipelines = 0
        if os.path.exists(f'{BasePipeline.folder}/pipeline/{namespace}'):
            total_pipelines = len(os.listdir(f'{BasePipeline.folder}/pipeline/{namespace}'))

        return {
            'schedules': Workspace.get_ppline_schedule(namespace),
            'ai_agent_namespace_details': ai_agent_namespace_details,
            'total_pipelines':  total_pipelines
        }
    
    except Exception as error:
        print(f'Error while trying to connect with AI agent')
        print(error)
        return 'failed'


@workspace.route('/workcpace/agent/<namespace>', methods=['GET'])
def start_ai_agent(namespace):
    try:
        return setup_agent(namespace)
    except Exception as error:
        print(f'Error while trying to connect with AI agent')
        print(error)
        return 'failed'
    

@workspace.route('/workcpace/agent/<namespace>/<username>', methods=['POST'])
def start_ai_agent_with_username(namespace, username):
    try:
        return setup_agent(namespace, username)
    except Exception as error:
        print(f'Error while trying to schedule schedule pipelines')
        print(error)
        return 'failed'


@workspace.route('/workcpace/agent/<namespace>', methods=['POST'])
def message_ai_agent(namespace):

    try:
        message_turn_limit = handle_conversasion_turn_limit(request, namespace)
        if message_turn_limit.get('error', None):
            return message_turn_limit

        payload = request.get_json()
        message = payload['message']

        if(os.path.exists(namespace)):
            result = 'No agent was started since no data found in the Namespace.'
            return { 'error': False, 'result': { 'result': result } }
        
        return send_message_to_agent_wit_groq(message, namespace)
    except Exception as error:
        print(f'AI Agent error while processing your request {str(error)}')
        print(error)
        traceback.print_exc()
        result = 'No medatata was loaded about your namespace.'\
              if str(error).strip() == namespace else 'Could not load details about your namespace.'
        return { 'error': True, 'result': { 'result': result } }
    
    
@workspace.route('/workcpace/agent/<namespace>/<username>', methods=['POST'])
def message_ai_agent_with_username(namespace, username):
    
    try:
        payload = request.get_json()
        message = payload['message']
        return send_message_to_agent(message, namespace, username)
    
    except Exception as error:
        print(f'AI Agent error while processing your request {str(error)}')
        print(error)
        return 'failed'
    

@workspace.route('/workcpace/ppline/job/schedule/', methods=['POST'])
@cross_origin(origins=[env('APP_SRV_ADDR')])
def setup_job_schedules():
    Workspace.schedule_pipeline_job()
    return ''


@workspace.route('/download/<type>/<namespace>/<filename>')
def download(type, namespace, filename):

    base_path = str(BasePipeline.folder).replace('/destinations','')
    ppline_path = f'{base_path}/{file_folder_map[type]}/{namespace}/'

    file_path = os.path.join(ppline_path, filename)
    if not os.path.exists(file_path):
        abort(404)
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=filename
    )

    
@workspace.route('/file/<namespace>/<filename>', methods=['DELETE'])
def delete_data_file(namespace, filename):

    ppline_path = f'{BaseUpload.upload_folder}/{namespace}/'
    file_to_remove = f'{ppline_path}/{filename}'

    try:
        os.remove(file_to_remove) if os.path.exists(file_to_remove) else None
        return { 'error': False, 'result': f'Files {filename} removed sucessfully.' }
    
    except Exception as err:
        print('Error while removing data file: ')
        print(err)
        traceback.print_exc()
        return { 'error': True, 'result': f'Error while removing data file: {str(err)}' }

    
import os
def call_scheduled_job():

    if os.path.exists('/.dockerenv'):
        # In case the app is running in Docker we call the schedul implementation 
        # straight instead of API call (else case),
        debounce_call_scheduled_job()
        
    else:
        def call_end_point():
            time.sleep(2)
            while True:
                response = requests.post(f'{env('APP_SRV_ADDR')}/workcpace/ppline/job/schedule/')
                response.raise_for_status()
                if response.status_code == 200 or response.status_code == 204:
                    break
                time.sleep(.5)

        task = threading.Thread(target=call_end_point)
        task.start()
    

def debounce_call_scheduled_job():

    def call_end_point():
        Workspace.schedule_pipeline_job()

    task = threading.Thread(target=call_end_point)
    task.start()


from services.agents import AgentFactory

def setup_agent(user, namespace = None):

    try:
        selected_namespace = namespace if namespace != None else user
        namespace_folder = BasePipeline.folder+f'/duckdb/{selected_namespace}'
        agent = AgentFactory.get_data_agent(user, namespace, namespace_folder)

        if agent == None:
            return { 
                'success': False, 
                'error': f'Could not start the Agent, as no data about the namespace exists.', 
                'start': False
            }

        return { 'error': False, 'success': True }
    except Exception as err:
        print(f'Error while staring the AI agent: {str(err)}')
        print(err.with_traceback)
        traceback.print_exc()
        return { 'error': f'Error while staring AI Agent: {str(err)}', 'success': False }
    

def send_message_to_agent(message, namespace, user_id = None):
    user = user_id if user_id != None else namespace
    agent = AgentFactory.get_data_agent(user)

    return { 'success': True, 'result': agent.cloud_mistral_call(message) }    


def send_message_to_agent_wit_groq(message, namespace, user_id = None):

    user = user_id if user_id != None else namespace
    agent = AgentFactory.get_data_agent(user)

    if(agent == None):
        return { 
            'success': False, 
            'result': "No agent was initiated since you don't/didn't have data in the namespace. I can update myself if you ask.",
            'started': False
        }
    

    if(not os.path.exists(agent.db_path)):
        return { 'success': False, 'result': 'No data, pipeline found in your name space.' }
    return { 'success': True, 'result': agent.cloud_groq_call(message) }


@workspace.route('/secret/<namespace>', methods=['POST'])
def create_seret(namespace):

    payload = request.get_json()

    try:
        
        secrets_only = False
        secret_type = ''

        if payload['apiSettings'] != None:
            path = payload['connectionName']
            del payload['dbConfig']
        else:
            path = payload['dbConfig']['connectionName']
            if 'secretsOnly' in payload['dbConfig']:
                secrets_only = True
            else:
                secret_type = 'db'
                
        sec_management: SecretManager = SecretManager.set_namespace(namespace, secret_type)

        if(secret_type == 'db'):
            sec_management.create_db_secret(namespace, payload, path)
        else:
            pre_path = f'main/api' if secrets_only == False else f'main/db'
            sec_management.create_secret(namespace, payload, f'{pre_path}/{path}')
        
        return { 'error': False, 'result': 'Secret created successfully' }
    except Exception as err:
        print('Error while secret creation: '+str(err))
        print(err)
        traceback.print_exc()
        return { 'error': True, 'result': f'Error while secret creation: {str(err)}' }


@workspace.route('/secret/<namespace>', methods=['GET'])
def list_serets(namespace):

    try:

        secret_names = SecretManager.list_secret_names(namespace)
        if secret_names == None:
            return { 'error': True, 'result': 'No secrete found for current namespace' }
        else:
            return { 'error': False, 'result': secret_names }
    except Exception as err:
        print('Error while fetching secrets list: '+str(err))
        print(err)
        traceback.print_exc()
        return { 'error': True, 'result': f'Error while fetching secrets list: {str(err)}' }


@workspace.route('/secret/<namespace>/<type>/<secretname>', methods=['GET'])
def fetch_secret(namespace, type, secretname):

    try:
        path = f'main/{type}/{secretname}'
        secret_details = SecretManager.get_secret(namespace,path=path,edit=True)
        if secret_details == None:
            return { 'error': True, 'result': 'No secrete found for current namespace' }
        else:
            return { 'error': False, 'result': secret_details }
    except Exception as err:
        print('Error while fetching secret: '+str(err))
        print(err)
        traceback.print_exc()
        return { 'error': True, 'result': f'Error while fetching secret: {str(err)}' }
    

@workspace.route('/<namespace>/db/connection/<connection_name>/tables', methods=['GET'])
def get_db_connection_detailes(namespace, connection_name):

    result = SQLDatabase.get_tables_list(namespace, connection_name)

    if 'error' not in result:
        return { 'error': False, 'result': { 'tables': result['tables'], 'secret_details': result['details'] } }
    else:
        return { 'error': True, 'result': 'No secrete found for current namespace' }
        

@workspace.route('/<namespace>/db/<dbengine>/<connection_name>/<table_name>', methods=['GET'])
def get_fields_from_db(namespace, dbengine, connection_name, table_name):

    result = SQLDatabase.get_fields_from_table(namespace, dbengine, connection_name, table_name)
    if 'error' not in result:
        return { 'error': False, 'result': { 'fields': result['fields'] } }
    else:
        return { 'error': True, 'result': 'No secrete found for current namespace' }
    

@workspace.route('/db/connection/<host>/<port>', methods=['GET'])
def get_ssl_dn(host, port):
    try:
        ssl_dn = Workspace.get_oracle_dn(host, port)
        return { 'error': False, 'result': ssl_dn }
    except Exception as err:
        return { 'error': True, 'result': str(err) }


@workspace.route('/workspace/connection/<exists_conn>/test', methods=['POST'])
@workspace.route('/workspace/connection/test', methods=['POST'])
def test_sql_db_connections(exists_conn = None):

    payload = request.get_json()
    config = payload['dbConfig']

    if(exists_conn == None):
        if('val1-db' in payload['env']):
            config['password'] = payload['env']['val1-db']

        if('key1-secret' in payload['env']):
            config['password'] = payload['env']['key1-secret']
    
    # To handle edge case of retesting existing 
    # connection/secret that is not Oracle type
    if 'dbConnectionParams' not in config:
        config['dbConnectionParams'] = ''

    dbengine = str(config['plugin_name']).split('-')[0]

    result = SQLDatabase.test_sql_connection(dbengine, config)
    
    return result