from os import getenv as env
from flask import Blueprint, request
from services.workspace.Workspace import Workspace
from controller.pipeline import BasePipeline
from flask_cors import cross_origin
import threading
import requests
import time
from services.DataQueryAIAssistent import DataQueryAIAssistent as Agent
from typing import List
import traceback
from flask import abort, send_file

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
def list_duck_dbs(user, socket_id):

    duckdb_path = BasePipeline.folder+'/duckdb/'+user+'/'
    dbs = Workspace.list_duck_dbs(duckdb_path, user)
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
            
    return dbs


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
        payload = request.get_json()
        message = payload['message']
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


agents_list: List[Agent] = {}
def setup_agent(user, namespace = None):

    try:
        if(not(user in agents_list)):
            selected_namespace = namespace if namespace != None else user
            namespace_folder = BasePipeline.folder+f'/duckdb/{selected_namespace}'
            agents_list[user] = Agent(namespace_folder)

        return { 'error': False, 'success': True }
    except Exception as err:
        print(f'Error while staring the AI agent: {str(err)}')
        print(err.with_traceback)
        traceback.print_exc()
        return { 'error': f'Error while staring AI Agent: {str(err)}', 'success': False }
    

def send_message_to_agent(message, namespace, user_id = None):
    user = user_id if user_id != None else namespace
    agent: Agent = agents_list[user]

    return { 'success': True, 'result': agent.cloud_mistral_call(message) }    


def send_message_to_agent_wit_groq(message, namespace, user_id = None):
    user = user_id if user_id != None else namespace
    agent: Agent = agents_list[user]

    return { 'success': True, 'result': agent.cloud_groq_call(message) }


