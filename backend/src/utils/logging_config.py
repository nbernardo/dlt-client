"""
Logging system configuration management.

This module provides centralized configuration management for the enhanced
pipeline logging system, including environment-specific settings, validation,
and default fallbacks.
"""

import os
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LoggingConfig:
    """
    Configuration settings for the enhanced logging system.
    """
    # Core logging settings
    log_level: str = "INFO"
    enable_console_logging: bool = True
    enable_file_logging: bool = True
    enable_database_logging: bool = True
    
    # Database settings
    database_path: Optional[str] = None
    max_database_connections: int = 10
    connection_timeout: int = 30
    
    # Retention settings
    default_retention_days: int = 30
    max_logs_per_pipeline: int = 10000
    cleanup_interval_hours: int = 24
    
    # Performance settings
    batch_size: int = 100
    flush_interval_seconds: int = 5
    max_queue_size: int = 1000
    
    # Fault tolerance settings
    enable_fault_tolerance: bool = True
    max_retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    
    # Fallback storage settings
    enable_fallback_storage: bool = True
    fallback_directory: str = "logs/fallback"
    fallback_max_files: int = 100
    
    # WebSocket integration
    enable_websocket_logging: bool = True
    websocket_buffer_size: int = 1000
    
    # Flask integration
    enable_flask_logging: bool = True
    log_request_details: bool = True
    log_response_details: bool = False
    
    # Pipeline-specific settings
    log_pipeline_stages: bool = True
    log_transformation_details: bool = True
    log_error_stack_traces: bool = True
    capture_environment_info: bool = True
    
    # Development settings
    debug_mode: bool = False
    log_sql_queries: bool = False
    enable_performance_metrics: bool = False


class LoggingConfigManager:
    """
    Manages logging configuration with environment variable support and validation.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_file: Optional path to configuration file
        """
        self._config = LoggingConfig()
        self._config_file = config_file
        self._logger = logging.getLogger(__name__)
        
        # Load configuration from environment and file
        self._load_configuration()
        self._validate_configuration()
    
    def _load_configuration(self):
        """Load configuration from environment variables and config file."""
        # Load from environment variables
        self._load_from_environment()
        
        # Load from config file if provided
        if self._config_file and os.path.exists(self._config_file):
            self._load_from_file()
    
    def _load_from_environment(self):
        """Load configuration from environment variables."""
        env_mappings = {
            # Core logging settings
            'LOGGING_LEVEL': ('log_level', str),
            'LOGGING_ENABLE_CONSOLE': ('enable_console_logging', self._parse_bool),
            'LOGGING_ENABLE_FILE': ('enable_file_logging', self._parse_bool),
            'LOGGING_ENABLE_DATABASE': ('enable_database_logging', self._parse_bool),
            
            # Database settings
            'LOGGING_DATABASE_PATH': ('database_path', str),
            'LOGGING_MAX_DB_CONNECTIONS': ('max_database_connections', int),
            'LOGGING_CONNECTION_TIMEOUT': ('connection_timeout', int),
            
            # Retention settings
            'LOGGING_RETENTION_DAYS': ('default_retention_days', int),
            'LOGGING_MAX_LOGS_PER_PIPELINE': ('max_logs_per_pipeline', int),
            'LOGGING_CLEANUP_INTERVAL_HOURS': ('cleanup_interval_hours', int),
            
            # Performance settings
            'LOGGING_BATCH_SIZE': ('batch_size', int),
            'LOGGING_FLUSH_INTERVAL': ('flush_interval_seconds', int),
            'LOGGING_MAX_QUEUE_SIZE': ('max_queue_size', int),
            
            # Fault tolerance settings
            'LOGGING_ENABLE_FAULT_TOLERANCE': ('enable_fault_tolerance', self._parse_bool),
            'LOGGING_MAX_RETRY_ATTEMPTS': ('max_retry_attempts', int),
            'LOGGING_RETRY_DELAY': ('retry_delay_seconds', float),
            'LOGGING_CIRCUIT_BREAKER_THRESHOLD': ('circuit_breaker_threshold', int),
            'LOGGING_CIRCUIT_BREAKER_TIMEOUT': ('circuit_breaker_timeout', int),
            
            # Fallback storage settings
            'LOGGING_ENABLE_FALLBACK': ('enable_fallback_storage', self._parse_bool),
            'LOGGING_FALLBACK_DIRECTORY': ('fallback_directory', str),
            'LOGGING_FALLBACK_MAX_FILES': ('fallback_max_files', int),
            
            # WebSocket integration
            'LOGGING_ENABLE_WEBSOCKET': ('enable_websocket_logging', self._parse_bool),
            'LOGGING_WEBSOCKET_BUFFER_SIZE': ('websocket_buffer_size', int),
            
            # Flask integration
            'LOGGING_ENABLE_FLASK': ('enable_flask_logging', self._parse_bool),
            'LOGGING_LOG_REQUEST_DETAILS': ('log_request_details', self._parse_bool),
            'LOGGING_LOG_RESPONSE_DETAILS': ('log_response_details', self._parse_bool),
            
            # Pipeline-specific settings
            'LOGGING_LOG_PIPELINE_STAGES': ('log_pipeline_stages', self._parse_bool),
            'LOGGING_LOG_TRANSFORMATION_DETAILS': ('log_transformation_details', self._parse_bool),
            'LOGGING_LOG_ERROR_STACK_TRACES': ('log_error_stack_traces', self._parse_bool),
            'LOGGING_CAPTURE_ENVIRONMENT_INFO': ('capture_environment_info', self._parse_bool),
            
            # Development settings
            'LOGGING_DEBUG_MODE': ('debug_mode', self._parse_bool),
            'LOGGING_LOG_SQL_QUERIES': ('log_sql_queries', self._parse_bool),
            'LOGGING_ENABLE_PERFORMANCE_METRICS': ('enable_performance_metrics', self._parse_bool),
        }
        
        for env_var, (attr_name, converter) in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value is not None:
                try:
                    converted_value = converter(env_value)
                    setattr(self._config, attr_name, converted_value)
                except (ValueError, TypeError) as e:
                    self._logger.warning(f"Invalid value for {env_var}: {env_value}. Using default. Error: {e}")
    
    def _load_from_file(self):
        """Load configuration from a file (JSON or similar)."""
        # This could be extended to support JSON, YAML, or other config formats
        # For now, we'll keep it simple and rely on environment variables
        pass
    
    def _parse_bool(self, value: str) -> bool:
        """Parse a string value to boolean."""
        if isinstance(value, bool):
            return value
        return str(value).lower() in ('true', '1', 'yes', 'on', 'enabled')
    
    def _validate_configuration(self):
        """Validate configuration values and apply constraints."""
        # Validate log level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self._config.log_level.upper() not in valid_levels:
            self._logger.warning(f"Invalid log level: {self._config.log_level}. Using INFO.")
            self._config.log_level = "INFO"
        else:
            self._config.log_level = self._config.log_level.upper()
        
        # Validate numeric constraints
        if self._config.default_retention_days < 1:
            self._logger.warning("Retention days must be at least 1. Using default.")
            self._config.default_retention_days = 30
        
        if self._config.max_logs_per_pipeline < 100:
            self._logger.warning("Max logs per pipeline must be at least 100. Using default.")
            self._config.max_logs_per_pipeline = 10000
        
        if self._config.batch_size < 1:
            self._logger.warning("Batch size must be at least 1. Using default.")
            self._config.batch_size = 100
        
        if self._config.max_retry_attempts < 0:
            self._logger.warning("Max retry attempts cannot be negative. Using default.")
            self._config.max_retry_attempts = 3
        
        # Validate paths
        if self._config.database_path:
            db_dir = Path(self._config.database_path).parent
            if not db_dir.exists():
                try:
                    db_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    self._logger.warning(f"Cannot create database directory: {e}. Using default.")
                    self._config.database_path = None
        
        # Validate fallback directory
        fallback_path = Path(self._config.fallback_directory)
        if not fallback_path.exists():
            try:
                fallback_path.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                self._logger.warning(f"Cannot create fallback directory: {e}. Using default.")
                self._config.fallback_directory = "logs/fallback"
    
    def get_config(self) -> LoggingConfig:
        """Get the current configuration."""
        return self._config
    
    def update_config(self, **kwargs):
        """Update configuration values."""
        for key, value in kwargs.items():
            if hasattr(self._config, key):
                setattr(self._config, key, value)
            else:
                self._logger.warning(f"Unknown configuration key: {key}")
        
        # Re-validate after updates
        self._validate_configuration()
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration."""
        return {
            'database_path': self._config.database_path,
            'max_connections': self._config.max_database_connections,
            'connection_timeout': self._config.connection_timeout,
            'batch_size': self._config.batch_size,
            'enable_database_logging': self._config.enable_database_logging,
        }
    
    def get_retention_config(self) -> Dict[str, Any]:
        """Get retention-specific configuration."""
        return {
            'default_retention_days': self._config.default_retention_days,
            'max_logs_per_pipeline': self._config.max_logs_per_pipeline,
            'cleanup_interval_hours': self._config.cleanup_interval_hours,
        }
    
    def get_fault_tolerance_config(self) -> Dict[str, Any]:
        """Get fault tolerance configuration."""
        return {
            'enable_fault_tolerance': self._config.enable_fault_tolerance,
            'max_retry_attempts': self._config.max_retry_attempts,
            'retry_delay_seconds': self._config.retry_delay_seconds,
            'circuit_breaker_threshold': self._config.circuit_breaker_threshold,
            'circuit_breaker_timeout': self._config.circuit_breaker_timeout,
            'enable_fallback_storage': self._config.enable_fallback_storage,
            'fallback_directory': self._config.fallback_directory,
            'fallback_max_files': self._config.fallback_max_files,
        }
    
    def get_performance_config(self) -> Dict[str, Any]:
        """Get performance-related configuration."""
        return {
            'batch_size': self._config.batch_size,
            'flush_interval_seconds': self._config.flush_interval_seconds,
            'max_queue_size': self._config.max_queue_size,
            'websocket_buffer_size': self._config.websocket_buffer_size,
        }
    
    def is_debug_mode(self) -> bool:
        """Check if debug mode is enabled."""
        return self._config.debug_mode
    
    def should_log_sql_queries(self) -> bool:
        """Check if SQL query logging is enabled."""
        return self._config.log_sql_queries
    
    def should_capture_performance_metrics(self) -> bool:
        """Check if performance metrics should be captured."""
        return self._config.enable_performance_metrics
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            field.name: getattr(self._config, field.name)
            for field in self._config.__dataclass_fields__.values()
        }
    
    def __str__(self) -> str:
        """String representation of configuration."""
        config_dict = self.to_dict()
        return f"LoggingConfig({', '.join(f'{k}={v}' for k, v in config_dict.items())})"


# Global configuration manager instance
_config_manager: Optional[LoggingConfigManager] = None


def get_logging_config() -> LoggingConfig:
    """
    Get the global logging configuration.
    
    Returns:
        Current logging configuration
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = LoggingConfigManager()
    return _config_manager.get_config()


def get_config_manager() -> LoggingConfigManager:
    """
    Get the global configuration manager.
    
    Returns:
        Configuration manager instance
    """
    global _config_manager
    if _config_manager is None:
        _config_manager = LoggingConfigManager()
    return _config_manager


def initialize_logging_config(config_file: Optional[str] = None) -> LoggingConfigManager:
    """
    Initialize the global logging configuration.
    
    Args:
        config_file: Optional path to configuration file
        
    Returns:
        Configuration manager instance
    """
    global _config_manager
    _config_manager = LoggingConfigManager(config_file)
    return _config_manager


def update_logging_config(**kwargs):
    """
    Update the global logging configuration.
    
    Args:
        **kwargs: Configuration values to update
    """
    config_manager = get_config_manager()
    config_manager.update_config(**kwargs)