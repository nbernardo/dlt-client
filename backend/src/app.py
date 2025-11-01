from pathlib import Path
import sys
from utils.env_util import set_env

proj_folder = Path(__file__).parent
set_env(proj_folder)

from flask import Flask, request
from flask_cors import CORS
from flask_socketio import emit
from controller.RequestContext import socketio

from controller.pipeline import pipeline, BasePipeline
from controller.workspace import workspace, call_scheduled_job
from services.workspace.SecretManager import SecretManager
from controller.file_upload import upload, BaseUpload
from utils.duckdb_util import DuckdbUtil
from utils import database_secret
from os import getenv as env
from utils.cache_util import DuckDBCache

BaseUpload.upload_folder = str(Path(__file__).parent.parent)+'/dbs/files'
BasePipeline.folder = str(Path(__file__).parent.parent)+'/destinations'
DuckdbUtil.workspacedb_path = str(Path(__file__).parent.parent)+'/dbs/files'
sys.path.insert(0, proj_folder/'node_mapper/')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hash#123098'

CORS(app)
socketio.init_app(app)

@socketio.on('connect', namespace='/pipeline')
def on_connect():
    emit('connected', {'sid': request.sid}, to=request.sid)
    socketio.sleep(0)

    

app.register_blueprint(pipeline)
app.register_blueprint(workspace)
app.register_blueprint(upload)

call_scheduled_job()

DuckDBCache.connect()
SecretManager.connect_to_vault()
SecretManager.db_secrete_obj = database_secret

port=env('APP_SRV_ADDR').split(':')[-1]
socketio.run(app, host="0.0.0.0", port=port, allow_unsafe_werkzeug=True)
