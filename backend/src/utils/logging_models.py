"""
Data models and interfaces for the pipeline logging enhancement system.

This module defines the core data structures and interfaces used throughout
the logging system for consistent data handling and type safety.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Protocol
import logging
import uuid


@dataclass
class EnrichedLogRecord:
    """
    Enhanced log record with pipeline execution context and metadata.
    
    This is the primary data structure used internally by the logging system
    to represent a fully processed log entry with all required context.
    """
    timestamp: datetime
    pipeline_id: str
    execution_id: str
    log_level: str
    logger_name: str
    message: str
    module: Optional[str] = None
    function_name: Optional[str] = None
    line_number: Optional[int] = None
    thread_id: Optional[int] = None
    process_id: Optional[int] = None
    correlation_id: Optional[str] = None
    extra_data: Optional[Dict[str, Any]] = None
    stack_trace: Optional[str] = None

    def __post_init__(self):
        """Ensure correlation_id is set if not provided."""
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4())


@dataclass
class LogStorageRecord:
    """
    Log record formatted specifically for DuckDB storage.
    
    This represents the final format that will be inserted into the database,
    with all fields properly serialized and validated.
    """
    timestamp: datetime
    pipeline_id: str
    execution_id: str
    log_level: str
    logger_name: str
    message: str
    module: Optional[str] = None
    function_name: Optional[str] = None
    line_number: Optional[int] = None
    thread_id: Optional[int] = None
    process_id: Optional[int] = None
    correlation_id: Optional[str] = None
    extra_data_json: Optional[str] = None  # JSON serialized extra_data
    stack_trace: Optional[str] = None
    created_at: Optional[datetime] = field(default_factory=datetime.now)


@dataclass
class LogQueryFilters:
    """
    Query filters for retrieving logs from storage.
    
    Provides a structured way to specify search criteria when querying
    the log database for specific entries.
    """
    pipeline_id: Optional[str] = None
    execution_id: Optional[str] = None
    log_levels: Optional[List[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    logger_name: Optional[str] = None
    message_pattern: Optional[str] = None
    correlation_id: Optional[str] = None
    hours: Optional[int] = None  # Number of hours to look back from now
    limit: int = 1000
    offset: int = 0

    def __post_init__(self):
        """Validate and normalize filter values."""
        if self.log_levels:
            # Normalize log levels to uppercase
            self.log_levels = [level.upper() for level in self.log_levels]
        
        if self.limit <= 0:
            self.limit = 1000
        
        if self.offset < 0:
            self.offset = 0
        
        # If hours is specified, set start_time automatically
        if self.hours is not None and self.hours > 0:
            if self.start_time is None:  # Don't override explicit start_time
                self.start_time = datetime.now() - timedelta(hours=self.hours)


@dataclass
class PipelineContext:
    """
    Context information for pipeline execution.
    
    Contains metadata about the current pipeline execution that should
    be included with all log entries from that execution.
    """
    pipeline_id: str
    execution_id: str
    namespace: Optional[str] = None
    user_id: Optional[str] = None
    pipeline_name: Optional[str] = None
    template_type: Optional[str] = None
    start_time: Optional[datetime] = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for JSON serialization."""
        return {
            'pipeline_id': self.pipeline_id,
            'execution_id': self.execution_id,
            'namespace': self.namespace,
            'user_id': self.user_id,
            'pipeline_name': self.pipeline_name,
            'template_type': self.template_type,
            'start_time': self.start_time.isoformat() if self.start_time else None
        }


class LogCaptureHandler(Protocol):
    """
    Interface for log capture handlers.
    
    Defines the contract for components that intercept and process
    log records from various sources (dltHub, application code, etc.).
    """
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        Process a log record from the Python logging system.
        
        Args:
            record: The log record to process
        """
        ...
    
    def set_pipeline_context(self, pipeline_id: str, execution_id: str) -> None:
        """
        Set the current pipeline execution context.
        
        Args:
            pipeline_id: Unique identifier for the pipeline
            execution_id: Unique identifier for this execution
        """
        ...
    
    def clear_context(self) -> None:
        """Clear the current pipeline context."""
        ...


class LogProcessor(Protocol):
    """
    Interface for log processing components.
    
    Defines the contract for components that enrich, format, and
    coordinate the storage of log records.
    """
    
    def process_log(self, record: logging.LogRecord, context: PipelineContext) -> None:
        """
        Process a log record with pipeline context.
        
        Args:
            record: The original log record
            context: Pipeline execution context
        """
        ...
    
    def enrich_record(self, record: logging.LogRecord, context: PipelineContext) -> EnrichedLogRecord:
        """
        Enrich a log record with context and metadata.
        
        Args:
            record: The original log record
            context: Pipeline execution context
            
        Returns:
            Enriched log record with full context
        """
        ...
    
    def format_for_storage(self, enriched_record: EnrichedLogRecord) -> LogStorageRecord:
        """
        Format an enriched record for database storage.
        
        Args:
            enriched_record: The enriched log record
            
        Returns:
            Storage-ready log record
        """
        ...


class LogStorage(Protocol):
    """
    Interface for log storage backends.
    
    Defines the contract for components that persist and retrieve
    log data from storage systems.
    """
    
    def initialize_tables(self) -> None:
        """Initialize storage tables and indexes."""
        ...
    
    def store_log(self, log_record: LogStorageRecord) -> None:
        """
        Store a log record in persistent storage.
        
        Args:
            log_record: The log record to store
        """
        ...
    
    def query_logs(self, filters: LogQueryFilters) -> List[Dict[str, Any]]:
        """
        Query logs from storage with filters.
        
        Args:
            filters: Query filters and pagination
            
        Returns:
            List of log records matching the criteria
        """
        ...
    
    def get_pipeline_logs(self, pipeline_id: str, execution_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all logs for a specific pipeline or execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional specific execution identifier
            
        Returns:
            List of log records for the pipeline/execution
        """
        ...


# Utility functions for working with log data

def create_execution_id() -> str:
    """Generate a unique execution ID for pipeline runs."""
    return f"exec_{uuid.uuid4().hex[:12]}"


def normalize_log_level(level: str) -> str:
    """Normalize log level to standard format."""
    level_mapping = {
        'CRITICAL': 'ERROR',  # Map CRITICAL to ERROR for simplicity
        'FATAL': 'ERROR',     # Map FATAL to ERROR for simplicity
    }
    normalized = level.upper()
    return level_mapping.get(normalized, normalized)


def extract_extra_data(record: logging.LogRecord) -> Dict[str, Any]:
    """
    Extract extra data from a log record.
    
    Args:
        record: The log record
        
    Returns:
        Dictionary of extra data fields
    """
    # Standard fields that should not be included in extra_data
    standard_fields = {
        'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
        'module', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
        'thread', 'threadName', 'processName', 'process', 'getMessage',
        'exc_info', 'exc_text', 'stack_info', 'message'
    }
    
    extra_data = {}
    for key, value in record.__dict__.items():
        if key not in standard_fields and not key.startswith('_'):
            # Convert non-serializable objects to strings
            try:
                # Test if value is JSON serializable
                import json
                json.dumps(value)
                extra_data[key] = value
            except (TypeError, ValueError):
                extra_data[key] = str(value)
    
    return extra_data if extra_data else None