from flask import Blueprint, request
from pathlib import Path
from services.workspace.Workspace import Workspace


workspace = Blueprint('workspace', __name__)

@workspace.route('/workcpace/code/run', methods=['POST'])
def run_code():

    payload = request.get_json()
    code = payload['code']

    if payload['lang'] == Workspace.editorLanguages['PYTHON']:
        output = Workspace.run_python_code(code)

    if payload['lang'] == Workspace.editorLanguages['SQL']:
        output = Workspace.execute_sql_query(code)
    
    return { 'output': output, 'lang': payload['code'] }


@workspace.route('/workcpace/duckdb/list', methods=['POST'])
def list_duck_dbs():
    dbs = Workspace.list_duck_dbs()
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