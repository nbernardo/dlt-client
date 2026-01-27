"""
Flask logging middleware for the pipeline logging enhancement system.

This module provides Flask middleware that integrates the enhanced logging system
with Flask application context, adding request context to application-level logs.
"""

import logging
import time
import uuid
from datetime import datetime
from flask import Flask, request, g, has_request_context
from typing import Optional, Dict, Any

from .logging_models import PipelineContext, create_execution_id
from .log_capture import ApplicationLogCaptureHandler, get_logging_context_manager
from .log_processor import get_log_processor, initialize_log_processor
from .log_storage import DuckDBLogStore


class FlaskLoggingMiddleware:
    """
    Flask middleware that integrates the enhanced logging system with Flask requests.
    
    This middleware:
    1. Adds request context to all application logs during request processing
    2. Tracks request lifecycle with structured logging
    3. Integrates with the existing log processor and storage system
    4. Maintains correlation IDs across request processing
    """
    
    def __init__(self, app: Flask = None, log_level: str = "INFO", 
                 enable_logging: bool = True, log_request_details: bool = True,
                 log_response_details: bool = False):
        """
        Initialize Flask logging middleware.
        
        Args:
            app: Flask application instance
            log_level: Minimum log level to capture
            enable_logging: Whether to enable Flask logging
            log_request_details: Whether to log detailed request information
            log_response_details: Whether to log detailed response information
        """
        self.app = app
        self.log_level = log_level.upper()
        self.enable_logging = enable_logging
        self.log_request_details = log_request_details
        self.log_response_details = log_response_details
        self.logger = logging.getLogger(__name__)
        self._log_handler: Optional[ApplicationLogCaptureHandler] = None
        
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app: Flask):
        """
        Initialize the middleware with a Flask application.
        
        Args:
            app: Flask application instance
        """
        self.app = app
        
        # Skip initialization if logging is disabled
        if not self.enable_logging:
            self.logger.info("Flask logging middleware disabled by configuration")
            return
        
        # Ensure log processor is initialized
        processor = get_log_processor()
        if processor is None:
            storage = DuckDBLogStore()
            processor = initialize_log_processor(storage)
        
        # Set up application-level log capture
        self._setup_application_logging()
        
        # Register Flask request hooks
        app.before_request(self._before_request)
        app.after_request(self._after_request)
        app.teardown_request(self._teardown_request)
        
        # Register error handler for logging
        app.errorhandler(Exception)(self._handle_exception)
        
        self.logger.info("Flask logging middleware initialized")
    
    def _setup_application_logging(self):
        """Set up application-level log capture for Flask."""
        try:
            # Create application log capture handler
            self._log_handler = ApplicationLogCaptureHandler()
            self._log_handler.setLevel(getattr(logging, self.log_level))
            
            # Add to Flask app logger
            flask_logger = logging.getLogger('werkzeug')
            flask_logger.addHandler(self._log_handler)
            flask_logger.setLevel(getattr(logging, self.log_level))
            
            # Add to application loggers
            app_loggers = [
                'controller.pipeline',
                'controller.workspace', 
                'controller.file_upload',
                'services.pipeline',
                'services.workspace',
                'utils.duckdb_util',
                'utils.SQLDatabase'
            ]
            
            for logger_name in app_loggers:
                logger = logging.getLogger(logger_name)
                logger.addHandler(self._log_handler)
                logger.setLevel(getattr(logging, self.log_level))
            
        except Exception as e:
            self.logger.error(f"Failed to set up application logging: {e}")
    
    def _before_request(self):
        """
        Hook called before each request.
        
        Sets up request context for logging.
        """
        try:
            # Generate request ID for correlation
            request_id = f"req_{uuid.uuid4().hex[:12]}"
            g.request_id = request_id
            g.request_start_time = time.time()
            
            # Create request context for logging
            request_context = {
                'request_id': request_id,
                'method': request.method,
                'path': request.path,
                'timestamp': datetime.now().isoformat()
            }
            
            # Add detailed request information if enabled
            if self.log_request_details:
                request_context.update({
                    'remote_addr': request.remote_addr,
                    'user_agent': request.headers.get('User-Agent', ''),
                    'content_type': request.content_type,
                    'content_length': request.content_length,
                    'query_string': request.query_string.decode('utf-8') if request.query_string else '',
                    'referrer': request.referrer
                })
            
            # Store in Flask g for access during request
            g.logging_context = request_context
            
            # Set logging context for this request thread
            if self._log_handler:
                self._log_handler.set_pipeline_context(
                    pipeline_id="flask_app",
                    execution_id=request_id,
                    namespace="http_request",
                    **request_context
                )
            
            # Log request start
            self.logger.info(f"Request started: {request.method} {request.path}", extra=request_context)
            
        except Exception as e:
            self.logger.error(f"Error in before_request: {e}")
    
    def _after_request(self, response):
        """
        Hook called after each request.
        
        Args:
            response: Flask response object
            
        Returns:
            The response object
        """
        try:
            if hasattr(g, 'request_start_time') and hasattr(g, 'logging_context'):
                # Calculate request duration
                duration = time.time() - g.request_start_time
                
                # Add response context
                response_context = g.logging_context.copy()
                response_context.update({
                    'status_code': response.status_code,
                    'duration_ms': round(duration * 1000, 2)
                })
                
                # Add detailed response information if enabled
                if self.log_response_details:
                    response_context.update({
                        'response_size': len(response.get_data()) if response.get_data() else 0,
                        'content_type': response.content_type,
                        'headers': dict(response.headers) if response.headers else {}
                    })
                
                # Log request completion
                log_level = 'WARNING' if response.status_code >= 400 else 'INFO'
                getattr(self.logger, log_level.lower())(
                    f"Request completed: {request.method} {request.path} - {response.status_code} ({duration:.3f}s)",
                    extra=response_context
                )
        
        except Exception as e:
            self.logger.error(f"Error in after_request: {e}")
        
        return response
    
    def _teardown_request(self, exception=None):
        """
        Hook called at the end of each request.
        
        Args:
            exception: Exception that occurred during request processing, if any
        """
        try:
            # Clear logging context for this request thread
            if self._log_handler:
                self._log_handler.clear_context()
            
            # Log any exception that occurred
            if exception:
                context = getattr(g, 'logging_context', {})
                self.logger.error(
                    f"Request failed with exception: {exception}",
                    extra=context,
                    exc_info=True
                )
        
        except Exception as e:
            self.logger.error(f"Error in teardown_request: {e}")
    
    def _handle_exception(self, error):
        """
        Handle exceptions that occur during request processing.
        
        Args:
            error: The exception that occurred
            
        Returns:
            None (lets Flask handle the error normally)
        """
        try:
            context = getattr(g, 'logging_context', {})
            context.update({
                'error_type': type(error).__name__,
                'error_message': str(error)
            })
            
            self.logger.error(
                f"Unhandled exception in request: {error}",
                extra=context,
                exc_info=True
            )
        
        except Exception as e:
            self.logger.error(f"Error in exception handler: {e}")
        
        # Re-raise the original exception to let Flask handle it
        raise error
    
    def get_request_context(self) -> Optional[Dict[str, Any]]:
        """
        Get the current request context for logging.
        
        Returns:
            Request context dictionary or None if not in request context
        """
        if has_request_context() and hasattr(g, 'logging_context'):
            return g.logging_context.copy()
        return None
    
    def add_request_context(self, **kwargs):
        """
        Add additional context to the current request logging context.
        
        Args:
            **kwargs: Additional context key-value pairs
        """
        if has_request_context() and hasattr(g, 'logging_context'):
            g.logging_context.update(kwargs)
            
            # Update the log handler context as well
            if self._log_handler:
                self._log_handler.set_pipeline_context(
                    pipeline_id="flask_app",
                    execution_id=g.get('request_id', 'unknown'),
                    namespace="http_request",
                    **g.logging_context
                )


def create_flask_logging_middleware(app: Flask, log_level: str = "INFO",
                                   enable_logging: bool = True, 
                                   log_request_details: bool = True,
                                   log_response_details: bool = False) -> FlaskLoggingMiddleware:
    """
    Factory function to create and initialize Flask logging middleware.
    
    Args:
        app: Flask application instance
        log_level: Minimum log level to capture
        enable_logging: Whether to enable Flask logging
        log_request_details: Whether to log detailed request information
        log_response_details: Whether to log detailed response information
        
    Returns:
        Configured FlaskLoggingMiddleware instance
    """
    middleware = FlaskLoggingMiddleware(
        app, log_level, enable_logging, log_request_details, log_response_details
    )
    return middleware


def get_current_request_context() -> Optional[Dict[str, Any]]:
    """
    Get the current request context from Flask g.
    
    Returns:
        Request context dictionary or None if not available
    """
    if has_request_context() and hasattr(g, 'logging_context'):
        return g.logging_context.copy()
    return None


def add_request_log_context(**kwargs):
    """
    Add context to the current request's logging context.
    
    Args:
        **kwargs: Context key-value pairs to add
    """
    if has_request_context() and hasattr(g, 'logging_context'):
        g.logging_context.update(kwargs)