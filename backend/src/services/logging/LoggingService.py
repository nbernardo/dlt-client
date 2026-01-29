"""
Dedicated Logging Service - Handles all logging operations.
Provides proper separation of concerns from pipeline services.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from utils.log_storage import DuckDBLogStore, create_log_storage_record
from utils.logging_models import EnrichedLogRecord, create_execution_id
from utils.logging_config import get_logging_config


class LoggingService:
    """
    Centralized service for managing all logging operations.
    Handles log storage, retrieval, and pipeline-specific logging.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.log_store = DuckDBLogStore()
        self.config = get_logging_config()
    
    def initialize_logging_system(self) -> bool:
        """
        Initialize the logging system tables and configuration.
        
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            self.log_store.initialize_tables()
            self.logger.info("Logging system initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize logging system: {e}")
            return False
    
    def create_pipeline_logger(self, pipeline_id: str, execution_id: Optional[str] = None) -> logging.Logger:
        """
        Create a logger specifically configured for a pipeline execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional execution identifier (creates new if None)
            
        Returns:
            Configured logger for the pipeline
        """
        if not execution_id:
            execution_id = create_execution_id()
        
        logger_name = f"pipeline.{pipeline_id}.{execution_id}"
        logger = logging.getLogger(logger_name)
        
        # Configure logger level
        logger.setLevel(getattr(logging, self.config.log_level))
        
        # Add custom attributes for pipeline context
        logger.pipeline_id = pipeline_id
        logger.execution_id = execution_id
        
        return logger
    
    def log_pipeline_event(self, 
                          pipeline_id: str, 
                          execution_id: str, 
                          message: str, 
                          level: str = "INFO",
                          extra_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Log a pipeline-specific event.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: The execution identifier
            message: Log message
            level: Log level (DEBUG, INFO, WARNING, ERROR)
            extra_data: Additional structured data
            
        Returns:
            True if logged successfully, False otherwise
        """
        try:
            enriched_record = EnrichedLogRecord(
                timestamp=datetime.now(),
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                log_level=level,
                logger_name=f"pipeline.{pipeline_id}",
                message=message,
                module="LoggingService",
                function_name="log_pipeline_event",
                line_number=0,
                extra_data=extra_data or {}
            )
            
            storage_record = create_log_storage_record(enriched_record)
            return self.log_store.store_log(storage_record)
            
        except Exception as e:
            self.logger.error(f"Failed to log pipeline event: {e}")
            return False
    
    def log_pipeline_start(self, pipeline_id: str, execution_id: str, config: Dict[str, Any] = None) -> bool:
        """
        Log the start of a pipeline execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: The execution identifier
            config: Pipeline configuration data
            
        Returns:
            True if logged successfully, False otherwise
        """
        extra_data = {
            "event_type": "pipeline_start",
            "config": config or {}
        }
        
        return self.log_pipeline_event(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            message=f"Pipeline '{pipeline_id}' execution started",
            level="INFO",
            extra_data=extra_data
        )
    
    def log_pipeline_end(self, 
                        pipeline_id: str, 
                        execution_id: str, 
                        success: bool = True,
                        duration: Optional[float] = None,
                        error_message: Optional[str] = None) -> bool:
        """
        Log the end of a pipeline execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: The execution identifier
            success: Whether the pipeline completed successfully
            duration: Execution duration in seconds
            error_message: Error message if failed
            
        Returns:
            True if logged successfully, False otherwise
        """
        status = "completed" if success else "failed"
        level = "INFO" if success else "ERROR"
        
        extra_data = {
            "event_type": "pipeline_end",
            "status": status,
            "success": success,
            "duration_seconds": duration
        }
        
        if error_message:
            extra_data["error_message"] = error_message
        
        message = f"Pipeline '{pipeline_id}' execution {status}"
        if duration:
            message += f" in {duration:.2f} seconds"
        
        return self.log_pipeline_event(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            message=message,
            level=level,
            extra_data=extra_data
        )
    
    def log_pipeline_progress(self, 
                             pipeline_id: str, 
                             execution_id: str, 
                             stage: str,
                             progress_data: Dict[str, Any] = None) -> bool:
        """
        Log pipeline progress information.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: The execution identifier
            stage: Current pipeline stage
            progress_data: Progress-specific data
            
        Returns:
            True if logged successfully, False otherwise
        """
        extra_data = {
            "event_type": "pipeline_progress",
            "stage": stage,
            **(progress_data or {})
        }
        
        return self.log_pipeline_event(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            message=f"Pipeline '{pipeline_id}' progress: {stage}",
            level="INFO",
            extra_data=extra_data
        )
    
    def get_pipeline_logs(self, pipeline_id: str, execution_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve logs for a specific pipeline.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional execution identifier
            
        Returns:
            List of log records
        """
        try:
            return self.log_store.get_pipeline_logs(pipeline_id, execution_id)
        except Exception as e:
            self.logger.error(f"Failed to retrieve pipeline logs: {e}")
            return []
    
    def get_pipeline_statistics(self, pipeline_id: Optional[str] = None, hours: int = 24) -> Dict[str, Any]:
        """
        Get logging statistics for monitoring.
        
        Args:
            pipeline_id: Optional pipeline to filter by
            hours: Number of hours to analyze
            
        Returns:
            Dictionary with statistics
        """
        try:
            return self.log_store.get_log_statistics(pipeline_id, hours)
        except Exception as e:
            self.logger.error(f"Failed to retrieve statistics: {e}")
            return {
                'error': str(e),
                'total_logs': 0,
                'unique_pipelines': 0,
                'log_levels': {}
            }
    
    def cleanup_old_logs(self, days_to_keep: int = 30, pipeline_id: Optional[str] = None) -> int:
        """
        Clean up old logs based on retention policy.
        
        Args:
            days_to_keep: Number of days to retain
            pipeline_id: Optional pipeline to clean
            
        Returns:
            Number of logs deleted
        """
        try:
            return self.log_store.cleanup_old_logs(days_to_keep, pipeline_id)
        except Exception as e:
            self.logger.error(f"Failed to cleanup logs: {e}")
            return 0


# Global logging service instance
_logging_service = None

def get_logging_service() -> LoggingService:
    """
    Get the global logging service instance.
    
    Returns:
        LoggingService instance
    """
    global _logging_service
    if _logging_service is None:
        _logging_service = LoggingService()
    return _logging_service