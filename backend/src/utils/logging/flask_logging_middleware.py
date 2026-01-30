import time
import uuid
import logging
from flask import request, g

class FlaskLoggingMiddleware:
    def __init__(self, app=None):
        self.logger = logging.getLogger("flask.app")
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        app.before_request(self._before_request)
        app.after_request(self._after_request)

    def _before_request(self):
        g.request_start_time = time.time()
        # Ensure every request has a unique ID for correlation
        g.request_id = f"req_{uuid.uuid4().hex[:10]}"

    def _after_request(self, response):
        duration = time.time() - getattr(g, 'request_start_time', time.time())
        
        # Capture metadata in 'extra' for the DuckDB handler
        log_context = {
            'pipeline_id': 'flask_server',
            'execution_id': g.request_id,
            'extra_data': {
                'method': request.method,
                'path': request.path,
                'status': response.status_code,
                'ms': round(duration * 1000, 2),
                'ip': request.remote_addr
            }
        }
        
        msg = f"{request.method} {request.path} - {response.status_code}"
        self.logger.info(msg, extra=log_context)
        return response