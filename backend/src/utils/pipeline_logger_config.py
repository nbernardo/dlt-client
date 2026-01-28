"""
Pipeline logging configuration for dltHub pipeline execution.

This module provides configuration management for pipeline-specific logging setup,
including dltHub logging configuration and context management.
"""

import logging
import os
from typing import Optional, Dict, Any, List
from datetime import datetime

from .logging_models import PipelineContext, create_execution_id
from .log_capture import (
    setup_dlt_logging_capture, 
    DltLogCaptureHandler, 
    pipeline_logging_context,
    get_logging_context_manager
)
from .log_processor import get_log_processor, initialize_log_processor
from .log_storage import DuckDBLogStore


class PipelineLoggerConfig:
    """
    Configuration management for pipeline-specific logging setup.
    
    This class handles the configuration of logging for dltHub pipeline execution,
    including logger setup, context management, and integration with the enhanced
    logging system.
    """
    
    def __init__(self, pipeline_id: str, log_level: str = "INFO", namespace: str = None):
        """
        Initialize pipeline logger configuration.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            log_level: Minimum log level to capture (DEBUG, INFO, WARNING, ERROR)
            namespace: Optional namespace for the pipeline
        """
        self.pipeline_id = pipeline_id
        self.log_level = log_level.upper()
        self.namespace = namespace or "default"
        self.execution_id = None
        self.context = None
        self.logger = logging.getLogger(__name__)
        
        # Track configured loggers for cleanup
        self._configured_loggers: List[str] = []
        self._capture_handlers: List[DltLogCaptureHandler] = []
        
        # Validate log level
        if self.log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            self.logger.warning(f"Invalid log level '{log_level}', defaulting to INFO")
            self.log_level = "INFO"
    
    def configure_dlt_logging(self, pipeline=None, execution_id: str = None, **kwargs) -> str:
        """
        Configure logging for dltHub pipeline execution.
        
        Args:
            pipeline: Optional dlt.Pipeline instance
            execution_id: Optional execution identifier (auto-generated if not provided)
            **kwargs: Additional context parameters (user_id, pipeline_name, template_type)
            
        Returns:
            The execution ID for this logging session
        """
        if execution_id is None:
            execution_id = create_execution_id()
        
        self.execution_id = execution_id
        
        # Create pipeline context
        self.context = PipelineContext(
            pipeline_id=self.pipeline_id,
            execution_id=execution_id,
            namespace=self.namespace,
            user_id=kwargs.get('user_id'),
            pipeline_name=kwargs.get('pipeline_name', self.pipeline_id),
            template_type=kwargs.get('template_type'),
            start_time=datetime.now()
        )
        
        try:
            # Ensure log processor is initialized
            processor = get_log_processor()
            if processor is None:
                # Initialize with default storage
                storage = DuckDBLogStore()
                processor = initialize_log_processor(storage)
            
            # Configure dltHub-specific logging
            self._configure_dlt_loggers()
            
            # Configure pipeline-specific loggers
            self._configure_pipeline_loggers(pipeline, **kwargs)
            
            # Start logging context management
            context_manager = get_logging_context_manager()
            context_manager.start_pipeline_logging(
                self.pipeline_id, 
                execution_id,
                **kwargs
            )
            
            self.logger.info(f"Configured logging for pipeline '{self.pipeline_id}' execution '{execution_id}'")
            
        except Exception as e:
            self.logger.error(f"Failed to configure dltHub logging: {e}")
            # Don't raise the exception to prevent pipeline execution failure
        
        return execution_id
    
    def configure_template_logging(self, template_context: dict) -> None:
        """
        Configure logging for template-specific contexts.
        
        Args:
            template_context: Dictionary containing template-specific information
        """
        try:
            # Extract template information
            template_type = template_context.get('template_type', 'unknown')
            template_name = template_context.get('template_name', 'unknown')
            
            # Update context if available
            if self.context:
                self.context.template_type = template_type
                if not self.context.pipeline_name or self.context.pipeline_name == self.pipeline_id:
                    self.context.pipeline_name = template_name
            
            # Configure template-specific loggers
            template_logger_name = f"template.{template_type}"
            template_logger = self.get_logger(template_logger_name)
            
            # Add template context to logger
            if hasattr(template_logger, 'addFilter'):
                template_filter = TemplateContextFilter(template_context)
                template_logger.addFilter(template_filter)
            
            self.logger.debug(f"Configured template logging for type '{template_type}'")
            
        except Exception as e:
            self.logger.error(f"Failed to configure template logging: {e}")
    
    def get_logger(self, name: str) -> logging.Logger:
        """
        Get a logger instance with proper configuration.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        
        # Configure logger if not already configured
        if name not in self._configured_loggers:
            logger.setLevel(getattr(logging, self.log_level))
            self._configured_loggers.append(name)
        
        return logger
    
    def _configure_dlt_loggers(self):
        """Configure dltHub-specific loggers."""
        try:
            # Configure main dlt logger
            dlt_logger = logging.getLogger('dlt')
            dlt_logger.setLevel(getattr(logging, self.log_level))
            
            # Configure dlt sub-loggers for more granular control
            dlt_subloggers = [
                'dlt.pipeline',
                'dlt.extract',
                'dlt.normalize',
                'dlt.load',
                'dlt.common',
                'dlt.destinations'
            ]
            
            for logger_name in dlt_subloggers:
                logger = logging.getLogger(logger_name)
                logger.setLevel(getattr(logging, self.log_level))
                self._configured_loggers.append(logger_name)
            
            # Add dlt logger to configured list
            self._configured_loggers.append('dlt')
            
        except Exception as e:
            self.logger.error(f"Failed to configure dlt loggers: {e}")
    
    def _configure_pipeline_loggers(self, pipeline=None, **kwargs):
        """Configure pipeline-specific loggers."""
        try:
            # Configure pipeline-specific logger
            pipeline_logger_name = f"pipeline.{self.pipeline_id}"
            pipeline_logger = self.get_logger(pipeline_logger_name)
            
            # Configure execution-specific logger
            if self.execution_id:
                execution_logger_name = f"execution.{self.execution_id}"
                execution_logger = self.get_logger(execution_logger_name)
            
            # Configure application loggers that might be used during pipeline execution
            app_loggers = [
                'services.pipeline',
                'controller.pipeline',
                'node_mapper',
                'utils.duckdb_util',
                'utils.SQLDatabase'
            ]
            
            for logger_name in app_loggers:
                logger = self.get_logger(logger_name)
            
        except Exception as e:
            self.logger.error(f"Failed to configure pipeline loggers: {e}")
    
    def set_log_level(self, log_level: str):
        """
        Update the log level for all configured loggers.
        
        Args:
            log_level: New log level (DEBUG, INFO, WARNING, ERROR)
        """
        log_level = log_level.upper()
        if log_level not in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            self.logger.warning(f"Invalid log level '{log_level}', keeping current level")
            return
        
        self.log_level = log_level
        level_obj = getattr(logging, log_level)
        
        # Update all configured loggers
        for logger_name in self._configured_loggers:
            logger = logging.getLogger(logger_name)
            logger.setLevel(level_obj)
        
        self.logger.info(f"Updated log level to {log_level} for pipeline '{self.pipeline_id}'")
    
    def add_custom_logger(self, logger_name: str, log_level: str = None) -> logging.Logger:
        """
        Add a custom logger with pipeline configuration.
        
        Args:
            logger_name: Name of the custom logger
            log_level: Optional specific log level for this logger
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(logger_name)
        level = log_level or self.log_level
        logger.setLevel(getattr(logging, level.upper()))
        
        if logger_name not in self._configured_loggers:
            self._configured_loggers.append(logger_name)
        
        return logger
    
    def get_context(self) -> Optional[PipelineContext]:
        """
        Get the current pipeline context.
        
        Returns:
            Current pipeline context or None if not configured
        """
        return self.context
    
    def update_context(self, **kwargs):
        """
        Update the current pipeline context with new information.
        
        Args:
            **kwargs: Context fields to update
        """
        if self.context:
            for key, value in kwargs.items():
                if hasattr(self.context, key):
                    setattr(self.context, key, value)
    
    def cleanup(self):
        """
        Clean up logging configuration and resources.
        
        This should be called when pipeline execution is complete.
        """
        try:
            # Stop logging context management
            if self.execution_id:
                context_manager = get_logging_context_manager()
                context_manager.stop_pipeline_logging(self.pipeline_id, self.execution_id)
            
            # Remove custom filters and handlers
            for handler in self._capture_handlers:
                # Remove handler from loggers
                for logger_name in self._configured_loggers:
                    logger = logging.getLogger(logger_name)
                    if handler in logger.handlers:
                        logger.removeHandler(handler)
            
            # Clear tracking lists
            self._configured_loggers.clear()
            self._capture_handlers.clear()
            
            # Reset context
            self.context = None
            self.execution_id = None
            
            self.logger.debug(f"Cleaned up logging configuration for pipeline '{self.pipeline_id}'")
            
        except Exception as e:
            self.logger.error(f"Error during logging cleanup: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get logging configuration statistics.
        
        Returns:
            Dictionary with configuration statistics
        """
        return {
            'pipeline_id': self.pipeline_id,
            'execution_id': self.execution_id,
            'log_level': self.log_level,
            'namespace': self.namespace,
            'configured_loggers_count': len(self._configured_loggers),
            'configured_loggers': self._configured_loggers.copy(),
            'capture_handlers_count': len(self._capture_handlers),
            'has_context': self.context is not None,
            'context_start_time': self.context.start_time.isoformat() if self.context and self.context.start_time else None
        }
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()


class TemplateContextFilter(logging.Filter):
    """
    Logging filter that adds template context to log records.
    """
    
    def __init__(self, template_context: dict):
        """
        Initialize template context filter.
        
        Args:
            template_context: Template context information
        """
        super().__init__()
        self.template_context = template_context
    
    def filter(self, record: logging.LogRecord) -> bool:
        """
        Add template context to log record.
        
        Args:
            record: Log record to filter
            
        Returns:
            True to allow the record to be processed
        """
        # Add template context as extra fields
        for key, value in self.template_context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        
        return True


def create_pipeline_logger_config(pipeline_id: str, log_level: str = "INFO", 
                                 namespace: str = None) -> PipelineLoggerConfig:
    """
    Factory function to create a pipeline logger configuration.
    
    Args:
        pipeline_id: Unique identifier for the pipeline
        log_level: Minimum log level to capture
        namespace: Optional namespace for the pipeline
        
    Returns:
        Configured PipelineLoggerConfig instance
    """
    return PipelineLoggerConfig(pipeline_id, log_level, namespace)


def configure_pipeline_logging(pipeline_id: str, execution_id: str = None, 
                              log_level: str = "INFO", namespace: str = None,
                              **kwargs) -> tuple[PipelineLoggerConfig, str]:
    """
    Convenience function to quickly configure pipeline logging.
    
    Args:
        pipeline_id: Unique identifier for the pipeline
        execution_id: Optional execution identifier
        log_level: Minimum log level to capture
        namespace: Optional namespace for the pipeline
        **kwargs: Additional context parameters
        
    Returns:
        Tuple of (PipelineLoggerConfig instance, execution_id)
    """
    config = PipelineLoggerConfig(pipeline_id, log_level, namespace)
    actual_execution_id = config.configure_dlt_logging(execution_id=execution_id, **kwargs)
    return config, actual_execution_id


# Global registry for active pipeline logger configurations
_active_configs: Dict[str, PipelineLoggerConfig] = {}


def register_pipeline_config(config: PipelineLoggerConfig):
    """Register a pipeline configuration globally."""
    key = f"{config.pipeline_id}:{config.execution_id}"
    _active_configs[key] = config


def unregister_pipeline_config(pipeline_id: str, execution_id: str):
    """Unregister a pipeline configuration."""
    key = f"{pipeline_id}:{execution_id}"
    if key in _active_configs:
        del _active_configs[key]


def get_pipeline_config(pipeline_id: str, execution_id: str = None) -> Optional[PipelineLoggerConfig]:
    """Get a registered pipeline configuration."""
    if execution_id:
        key = f"{pipeline_id}:{execution_id}"
        return _active_configs.get(key)
    
    # If no execution_id provided, find any config for the pipeline
    for key, config in _active_configs.items():
        if config.pipeline_id == pipeline_id:
            return config
    
    return None


def cleanup_all_pipeline_configs():
    """Clean up all registered pipeline configurations."""
    for config in list(_active_configs.values()):
        config.cleanup()
    _active_configs.clear()