from flask import Blueprint, request
import uuid
import subprocess
from pathlib import Path
from services.workspace.Workspace import Workspace


workspace = Blueprint('workspace', __name__)

@workspace.route('/workcpace/code/run', methods=['POST'])
def run_code():
    payload = request.get_json()
    return {
        'output': Workspace.run_code(payload),
    }