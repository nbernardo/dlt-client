from pathlib import Path
import sys
from utils.env_util import set_env

from flask import Flask, request
from flask_cors import CORS
from flask_socketio import emit
from controller.RequestContext import socketio

from controller.pipeline import pipeline
from controller.workspace import workspace

proj_folder = Path(__file__).parent
sys.path.insert(0, proj_folder/'node_mapper/')
set_env(proj_folder)

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
socketio.run(app, host="0.0.0.0", port=8000)
