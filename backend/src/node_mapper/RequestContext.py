from flask_socketio import emit


class RequestContext:

    step_error = 'pplineError'
    step_success = 'pplineStepSuccess'
    namespace = '/pipeline'

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
            {'componentId': obj.component_id, 'error': error},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )

    def emit_success(self, obj: object, data):
        """
        This emit Websocket success message for a
        specific pipeline execution step
        """
        emit(
            RequestContext.step_success,
            {'componentId': obj.component_id, 'error': data},
            to=self.socket_sid,
            namespace=RequestContext.namespace
        )
