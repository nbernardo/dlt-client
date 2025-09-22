from os import getenv as env
from flask import Blueprint, request
from services.workspace.Workspace import Workspace
from controller.pipeline import BasePipeline
from flask_cors import cross_origin
import threading
import requests
import time


workspace = Blueprint('workspace', __name__)
schedule_was_called = None

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

        schedule.every(1).minutes.do(DltPipeline.run_pipeline_job, file_path, namespace)
        schedule.every(20).seconds.do(lambda: print(f'Preparing to run job for {file_path} pipeline'))

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
        print(f'Error while trying to schedule schedule pipelines')
        print(error)
        return 'failed'
    

@workspace.route('/workcpace/ppline/job/schedule/', methods=['POST'])
@cross_origin(origins=[env('APP_SRV_ADDR')])
def setup_job_schedules():
    Workspace.schedule_pipeline_job()
    return ''
    

def call_scheduled_job():
    
    def call_end_point():
        time.sleep(10)
        while True:
            response = requests.post(f'{env('APP_SRV_ADDR')}/workcpace/ppline/job/schedule/')
            response.raise_for_status()
            if response.status_code == 200 or response.status_code == 204:
                break
            time.sleep(2)

    task = threading.Thread(target=call_end_point)
    task.start()