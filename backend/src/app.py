from pathlib import Path
import sys

from flask import Flask, request
from controller.pipeline import pipeline
from flask_cors import CORS
from flask_socketio import SocketIO, emit, Namespace


class PiplineNamespace(Namespace):
    def on_connect(self): pass
    def on_disconnect(self, reason): pass


proj_folder = Path(__file__).parent
sys.path.insert(0, proj_folder/'node_mapper/')

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hash#123098'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="http://127.0.0.1:8080",
                    logger=True, engineio_logger=True)

socketio.on_namespace(PiplineNamespace('/pipeline'))


@socketio.on('connect', namespace='/pipeline')
def on_connect():
    emit('connected', {'sid': request.sid}, to=request.sid)


app.register_blueprint(pipeline)
socketio.run(app)
