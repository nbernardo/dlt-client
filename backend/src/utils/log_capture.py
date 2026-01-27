"""
Log capture handlers for intercepting logs from various sources.

This module provides logging handlers that intercept log records from
dltHub pipelines and application code, forwarding them to the log processor
for enrichment and storage.
"""

import logging
import threading
from typing import Optional, Dict, Any
from contextlib import contextmanager

from .logging_models import PipelineContext, create_execution_id
from .log_processor import get_log_processor


class LogCaptureHandler(logging.Handler):
    """
    Custom Python logging handler that intercepts and processes log records.
    
    This handler captures log records from both dltHub and application sources,
    adding pipeline execution context and forwarding them to the log processor.
    """
    
    def __init__(self, level=logging.NOTSET):
        """
        Initialize the log capture handler.
        
        Args:
            level: Minimum log level to handle
        """
        super().__init__(level)
        self._context_storage = threading.local()
        self._default_context = None
        self.logger = logging.getLogger(__name__)
        
        # Prevent infinite recursion by not processing our own logs
        self.addFilter(self._self_filter)
    
    def _self_filter(self, record: logging.LogRecord) -> bool:
        """Filter to prevent processing our own log messages."""
        # Don't process logs from the logging system itself to prevent recursion
        return not record.name.startswith('utils.log_')
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        Process a log record from the Python logging system with fault tolerance.
        
        Args:
            record: The log record to process
        """
        try:
            # Get the current pipeline context
            context = self.get_current_context()
            
            if context is None:
                # No context available, skip processing or use default
                if self._default_context:
                    context = self._default_context
                else:
                    # Create a minimal context for application-level logs
                    context = PipelineContext(
                        pipeline_id="application",
                        execution_id="app_session",
                        namespace="default"
                    )
            
            # Get the log processor and process the record
            processor = get_log_processor()
            if processor:
                processor.process_log(record, context)
            else:
                # Log processor not available, this might happen during startup
                # Don't log this as it could cause recursion during initialization
                pass
        
        except Exception as e:
            # Don't let logging errors disrupt the application
            # Use print as a last resort to avoid recursion
            try:
                self.logger.debug(f"Error processing log record: {e}")
            except:
                # Even logging the error failed, use print as absolute fallback
                print(f"Critical logging error: {e}", flush=True)
    
    def set_pipeline_context(self, pipeline_id: str, execution_id: str, **kwargs) -> None:
        """
        Set the current pipeline execution context for this thread.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            execution_id: Unique identifier for this execution
            **kwargs: Additional context parameters
        """
        context = PipelineContext(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            namespace=kwargs.get('namespace'),
            user_id=kwargs.get('user_id'),
            pipeline_name=kwargs.get('pipeline_name'),
            template_type=kwargs.get('template_type')
        )
        
        self._context_storage.context = context
    
    def get_current_context(self) -> Optional[PipelineContext]:
        """
        Get the current pipeline context for this thread.
        
        Returns:
            Current pipeline context or None if not set
        """
        return getattr(self._context_storage, 'context', None)
    
    def clear_context(self) -> None:
        """Clear the current pipeline context for this thread."""
        if hasattr(self._context_storage, 'context'):
            delattr(self._context_storage, 'context')
    
    def set_default_context(self, pipeline_id: str, execution_id: str, **kwargs) -> None:
        """
        Set a default context to use when no thread-specific context is available.
        
        Args:
            pipeline_id: Default pipeline identifier
            execution_id: Default execution identifier
            **kwargs: Additional context parameters
        """
        self._default_context = PipelineContext(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            namespace=kwargs.get('namespace', 'default'),
            user_id=kwargs.get('user_id'),
            pipeline_name=kwargs.get('pipeline_name'),
            template_type=kwargs.get('template_type')
        )


class DltLogCaptureHandler(LogCaptureHandler):
    """
    Specialized log capture handler for dltHub pipeline logs.
    
    This handler is specifically designed to capture logs from dltHub
    pipeline execution with enhanced context management.
    """
    
    def __init__(self, pipeline_id: str, execution_id: str = None, level=logging.NOTSET):
        """
        Initialize the dltHub log capture handler.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional execution identifier (auto-generated if not provided)
            level: Minimum log level to handle
        """
        super().__init__(level)
        
        if execution_id is None:
            execution_id = create_execution_id()
        
        # Set the pipeline context immediately
        self.set_pipeline_context(pipeline_id, execution_id)
        self.pipeline_id = pipeline_id
        self.execution_id = execution_id
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        Process a dltHub log record with enhanced context.
        
        Args:
            record: The log record to process
        """
        # Add dltHub-specific context to the record
        if not hasattr(record, 'pipeline_id'):
            record.pipeline_id = self.pipeline_id
        if not hasattr(record, 'execution_id'):
            record.execution_id = self.execution_id
        
        # Add dltHub source marker
        if not hasattr(record, 'source'):
            record.source = 'dltHub'
        
        super().emit(record)


class ApplicationLogCaptureHandler(LogCaptureHandler):
    """
    Log capture handler for general application logs.
    
    This handler captures logs from Flask controllers, services, and other
    application components that are not part of pipeline execution.
    """
    
    def __init__(self, level=logging.NOTSET):
        """
        Initialize the application log capture handler.
        
        Args:
            level: Minimum log level to handle
        """
        super().__init__(level)
        
        # Set a default context for application logs
        self.set_default_context(
            pipeline_id="application",
            execution_id="app_session",
            namespace="application"
        )
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        Process an application log record.
        
        Args:
            record: The log record to process
        """
        # Add application source marker
        if not hasattr(record, 'source'):
            record.source = 'application'
        
        super().emit(record)


@contextmanager
def pipeline_logging_context(pipeline_id: str, execution_id: str = None, **kwargs):
    """
    Context manager for setting pipeline logging context.
    
    Args:
        pipeline_id: The pipeline identifier
        execution_id: Optional execution identifier (auto-generated if not provided)
        **kwargs: Additional context parameters
    
    Usage:
        with pipeline_logging_context("my_pipeline", execution_id="exec_123"):
            logger.info("This log will have pipeline context")
    """
    if execution_id is None:
        execution_id = create_execution_id()
    
    # Get all log capture handlers in the root logger
    root_logger = logging.getLogger()
    capture_handlers = [
        handler for handler in root_logger.handlers
        if isinstance(handler, LogCaptureHandler)
    ]
    
    # Set context on all capture handlers
    for handler in capture_handlers:
        handler.set_pipeline_context(pipeline_id, execution_id, **kwargs)
    
    try:
        yield execution_id
    finally:
        # Clear context on all capture handlers
        for handler in capture_handlers:
            handler.clear_context()


def setup_logging_capture(log_level: str = "INFO") -> LogCaptureHandler:
    """
    Set up log capture for the application.
    
    Args:
        log_level: Minimum log level to capture
        
    Returns:
        The configured log capture handler
    """
    # Create and configure the main log capture handler
    handler = ApplicationLogCaptureHandler()
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # Add to root logger to capture all logs
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    
    # Ensure root logger level allows our handler to receive logs
    if root_logger.level > handler.level:
        root_logger.setLevel(handler.level)
    
    return handler


def setup_dlt_logging_capture(pipeline_id: str, execution_id: str = None, 
                             log_level: str = "INFO", **kwargs) -> DltLogCaptureHandler:
    """
    Set up log capture specifically for dltHub pipeline execution.
    
    Args:
        pipeline_id: The pipeline identifier
        execution_id: Optional execution identifier
        log_level: Minimum log level to capture
        
    Returns:
        The configured dltHub log capture handler
    """
    if execution_id is None:
        execution_id = create_execution_id()
    
    # Create dltHub-specific handler
    handler = DltLogCaptureHandler(pipeline_id, execution_id)
    handler.setLevel(getattr(logging, log_level.upper()))
    
    # Add to dlt logger specifically
    dlt_logger = logging.getLogger('dlt')
    dlt_logger.addHandler(handler)
    
    # Also add to root logger to catch any dlt logs that don't use the dlt logger
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    
    return handler


def remove_logging_capture():
    """Remove all log capture handlers from loggers."""
    # Remove from root logger
    root_logger = logging.getLogger()
    handlers_to_remove = [
        handler for handler in root_logger.handlers
        if isinstance(handler, LogCaptureHandler)
    ]
    
    for handler in handlers_to_remove:
        root_logger.removeHandler(handler)
    
    # Remove from dlt logger
    dlt_logger = logging.getLogger('dlt')
    dlt_handlers_to_remove = [
        handler for handler in dlt_logger.handlers
        if isinstance(handler, LogCaptureHandler)
    ]
    
    for handler in dlt_handlers_to_remove:
        dlt_logger.removeHandler(handler)


class LoggingContextManager:
    """
    Manager for logging contexts across the application.
    
    This class provides centralized management of logging contexts,
    ensuring proper setup and cleanup of log capture handlers.
    """
    
    def __init__(self):
        """Initialize the logging context manager."""
        self._active_handlers: Dict[str, LogCaptureHandler] = {}
        self._lock = threading.Lock()
    
    def start_pipeline_logging(self, pipeline_id: str, execution_id: str = None, 
                              **kwargs) -> str:
        """
        Start logging for a pipeline execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional execution identifier
            **kwargs: Additional context parameters
            
        Returns:
            The execution ID for this logging session
        """
        if execution_id is None:
            execution_id = create_execution_id()
        
        with self._lock:
            # Create handler key
            handler_key = f"{pipeline_id}:{execution_id}"
            
            # Remove existing handler if present
            if handler_key in self._active_handlers:
                self.stop_pipeline_logging(pipeline_id, execution_id)
            
            # Create new handler
            handler = setup_dlt_logging_capture(pipeline_id, execution_id, **kwargs)
            self._active_handlers[handler_key] = handler
        
        return execution_id
    
    def stop_pipeline_logging(self, pipeline_id: str, execution_id: str):
        """
        Stop logging for a pipeline execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: The execution identifier
        """
        with self._lock:
            handler_key = f"{pipeline_id}:{execution_id}"
            
            if handler_key in self._active_handlers:
                handler = self._active_handlers[handler_key]
                
                # Remove handler from loggers
                root_logger = logging.getLogger()
                if handler in root_logger.handlers:
                    root_logger.removeHandler(handler)
                
                dlt_logger = logging.getLogger('dlt')
                if handler in dlt_logger.handlers:
                    dlt_logger.removeHandler(handler)
                
                # Remove from active handlers
                del self._active_handlers[handler_key]
    
    def get_active_handlers(self) -> Dict[str, LogCaptureHandler]:
        """Get all currently active log capture handlers."""
        with self._lock:
            return self._active_handlers.copy()
    
    def cleanup_all(self):
        """Clean up all active logging handlers."""
        with self._lock:
            for handler_key in list(self._active_handlers.keys()):
                pipeline_id, execution_id = handler_key.split(':', 1)
                self.stop_pipeline_logging(pipeline_id, execution_id)


# Global logging context manager
_logging_context_manager = LoggingContextManager()


def get_logging_context_manager() -> LoggingContextManager:
    """Get the global logging context manager."""
    return _logging_context_manager