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
        output = Workspace.run_sql_code(code)

    return { 'output': output, 'lang': payload['code'] }