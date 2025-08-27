from flask import Blueprint, request
from pathlib import Path
from services.workspace.Workspace import Workspace
from controller.pipeline import BasePipeline

workspace = Blueprint('workspace', __name__)

@workspace.route('/workcpace/code/run/<user>', methods=['POST'])
def run_code(user):

    payload = request.get_json()
    code = payload['code']

    if payload['lang'] == Workspace.editorLanguages['PYTHON']:
        output = Workspace.run_python_code(code)

    if payload['lang'] == Workspace.editorLanguages['SQL']:
        output = Workspace.execute_sql_query(code, user)
    
    return { 'output': output, 'lang': payload['code'] }


@workspace.route('/workcpace/duckdb/list/<user>', methods=['POST'])
def list_duck_dbs(user):
    duckdb_path = BasePipeline.folder+'/duckdb/'+user+'/'
    dbs = Workspace.list_duck_dbs(duckdb_path)
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