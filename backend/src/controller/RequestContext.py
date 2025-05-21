from flask_socketio import SocketIO, emit, Namespace


class PiplineNamespace(Namespace):
    def on_connect(self): pass
    def on_disconnect(self, reason): pass

socketio = SocketIO(cors_allowed_origins=["http://127.0.0.1:8080", "http://localhost:8080"],
                    logger=True, engineio_logger=True, async_mode='eventlet')

socketio.on_namespace(PiplineNamespace('/pipeline'))

class RequestContext:

    step_error = 'pplineError'
    step_success = 'pplineStepSuccess'
    step_start = 'pplineStepStart'
    namespace = '/pipeline'
    ppline_success = 'pplineSuccess'
    FAILED = 'FAILED'

    def __init__(self, ppline_name=None, socket_sid=None):
        self.ppline_name = ppline_name
        self.exceptions = []
        self.ppline_files_path = "/home/nakassony/dlt-project/backend/src"
        self.socket_sid = socket_sid

    def add_exception(self, type, error):
        """
        Register any likely error from pipeline phase
        """
        self.exceptions.append({type: error})

    def emit_error(self, obj: object, error):
        """
        This emit Websocket error message for a
        specific pipeline execution step
        """
        emit(
            RequestContext.step_error,
            {'componentId': obj.component_id,
                'sid': self.socket_sid, 'error': error},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )
        socketio.sleep(0)


    def emit_success(self, obj: object, data):
        """
        This emit Websocket success message for a
        specific pipeline execution step
        """
        emit(
            RequestContext.step_success,
            {'componentId': obj.component_id,
                'data': data, 'sid': self.socket_sid},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )
        socketio.sleep(0)


    def emit_start(self, obj: object, data):
        """
        This emit Websocket success message for a
        specific pipeline execution step
        """
        emit(
            RequestContext.step_start,
            {'componentId': obj.component_id, 'data': data,
                'sid': self.socket_sid},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )
        socketio.sleep(0)
        

    def emit_ppsuccess(self, data=True, socked_sid=None):
        """
        This emit Websocket success message for a
        specific pipeline execution step
        """
        emit(
            RequestContext.ppline_success,
            {'success': data, 'sid': self.socket_sid},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )
        socketio.sleep(0)
