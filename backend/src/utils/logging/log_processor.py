from logging.handlers import QueueHandler, QueueListener
from queue import Queue
from .log_storage import DuckDBLogStore
import logging
from .flask_logging_middleware import FlaskLoggingMiddleware

class AsyncDuckDBHandler(logging.Handler):
    """Bridge between the Queue and the Store."""
    def __init__(self, store: DuckDBLogStore):
        super().__init__()
        self.store = store

    def emit(self, record):
        # We handle batching here or simply pass to the store
        self.store.store_logs_batch([record])

import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

def setup_logging(app=None):
    
    log_queue = Queue(-1)
    store = DuckDBLogStore()
    
    listener = QueueListener(log_queue, AsyncDuckDBHandler(store))
    listener.start()

    root = logging.getLogger() 
    root.setLevel(logging.INFO)
    
    for h in root.handlers[:]:
        root.removeHandler(h)
        
    root.addHandler(QueueHandler(log_queue))

    if app:
        FlaskLoggingMiddleware(app)
