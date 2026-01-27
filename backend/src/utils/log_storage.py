"""
DuckDB-based log storage implementation for the pipeline logging system.

This module provides persistent storage for pipeline logs using DuckDB,
with efficient querying, robust error handling, and fault tolerance.
"""

import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import asdict

from .duckdb_util import DuckdbUtil
from .logging_models import LogStorageRecord, LogQueryFilters, EnrichedLogRecord
from .logging_config import get_logging_config
from .fault_tolerance import (
    FaultTolerantLogger, RetryConfig, CircuitBreakerConfig, 
    FallbackStorageConfig, StorageUnavailableError
)


class DuckDBLogStore:
    """
    DuckDB-based implementation of log storage with fault tolerance.
    
    Provides persistent storage for pipeline logs with efficient querying
    capabilities, robust error handling, and graceful degradation.
    """
    
    def __init__(self, 
                 enable_fault_tolerance: Optional[bool] = None,
                 retry_config: Optional[RetryConfig] = None,
                 circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
                 fallback_config: Optional[FallbackStorageConfig] = None):
        """
        Initialize the DuckDB log store with optional fault tolerance.
        
        Args:
            enable_fault_tolerance: Whether to enable fault tolerance features (uses config if None)
            retry_config: Retry configuration for transient failures
            circuit_breaker_config: Circuit breaker configuration
            fallback_config: Fallback storage configuration
        """
        self.logger = logging.getLogger(__name__)
        self._initialized = False
        
        # Get configuration
        config = get_logging_config()
        
        # Use configuration values if not explicitly provided
        self.enable_fault_tolerance = (
            enable_fault_tolerance if enable_fault_tolerance is not None 
            else config.enable_fault_tolerance
        )
        
        # Initialize fault tolerance if enabled
        if self.enable_fault_tolerance:
            # Use configuration for fault tolerance settings if not provided
            if retry_config is None:
                retry_config = RetryConfig(
                    max_attempts=config.max_retry_attempts,
                    base_delay=config.retry_delay_seconds,
                    max_delay=config.retry_delay_seconds * 10,
                    exponential_base=2.0,
                    jitter=True
                )
            
            if circuit_breaker_config is None:
                circuit_breaker_config = CircuitBreakerConfig(
                    failure_threshold=config.circuit_breaker_threshold,
                    recovery_timeout=config.circuit_breaker_timeout,
                    success_threshold=3
                )
            
            if fallback_config is None and config.enable_fallback_storage:
                fallback_config = FallbackStorageConfig(
                    enabled=True,
                    temp_dir=config.fallback_directory,
                    max_files=config.fallback_max_files,
                    max_file_size=10 * 1024 * 1024  # 10MB
                )
            
            self.fault_tolerant_logger = FaultTolerantLogger(
                primary_storage_func=self._store_log_direct,
                retry_config=retry_config,
                circuit_breaker_config=circuit_breaker_config,
                fallback_config=fallback_config
            )
        else:
            self.fault_tolerant_logger = None
    
    def initialize_tables(self) -> None:
        """
        Initialize storage tables and indexes.
        
        This method is idempotent and can be called multiple times safely.
        """
        try:
            DuckdbUtil.initialize_logging_tables()
            self._initialized = True
            self.logger.info("DuckDB log storage initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize log storage: {e}")
            # Don't raise the exception to prevent application startup failure
            # Logging should be optional and not break the main application
    
    def store_log(self, log_record: LogStorageRecord) -> bool:
        """
        Store a log record in persistent storage with fault tolerance.
        
        Args:
            log_record: The log record to store
            
        Returns:
            True if stored successfully (primary or fallback), False otherwise
        """
        if not self._initialized:
            self.initialize_tables()
        
        if self.enable_fault_tolerance and self.fault_tolerant_logger:
            # Use fault-tolerant storage
            return self.fault_tolerant_logger.store_log(log_record)
        else:
            # Use direct storage (legacy mode)
            try:
                self._store_log_direct(log_record)
                return True
            except Exception as e:
                self.logger.error(f"Failed to store log record: {e}")
                return False
    
    def _store_log_direct(self, log_record: LogStorageRecord) -> None:
        """
        Store a log record directly in DuckDB (used by fault tolerance system).
        
        Args:
            log_record: The log record to store
            
        Raises:
            StorageUnavailableError: If storage is unavailable
            Exception: For other storage errors
        """
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            # Prepare the insert query
            query = """
                INSERT INTO pipeline_logs (
                    timestamp, pipeline_id, execution_id, log_level, logger_name,
                    message, module, function_name, line_number, thread_id,
                    process_id, correlation_id, extra_data, stack_trace, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Execute the insert
            cnx.execute(query, (
                log_record.timestamp,
                log_record.pipeline_id,
                log_record.execution_id,
                log_record.log_level,
                log_record.logger_name,
                log_record.message,
                log_record.module,
                log_record.function_name,
                log_record.line_number,
                log_record.thread_id,
                log_record.process_id,
                log_record.correlation_id,
                log_record.extra_data_json,
                log_record.stack_trace,
                log_record.created_at
            ))
            
        except Exception as e:
            # Check if this is a connection/availability issue
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ['connection', 'database', 'locked', 'busy']):
                raise StorageUnavailableError(f"Storage unavailable: {e}") from e
            else:
                # Re-raise other exceptions as-is
                raise
    
    def store_logs_batch(self, log_records: List[LogStorageRecord]) -> Dict[str, Any]:
        """
        Store multiple log records in a single transaction for efficiency.
        
        Args:
            log_records: List of log records to store
            
        Returns:
            Dictionary with batch storage results
        """
        if not log_records:
            return {'success': True, 'stored_count': 0, 'failed_count': 0}
        
        if not self._initialized:
            self.initialize_tables()
        
        stored_count = 0
        failed_count = 0
        
        if self.enable_fault_tolerance and self.fault_tolerant_logger:
            # Store each record individually with fault tolerance
            for record in log_records:
                if self.fault_tolerant_logger.store_log(record):
                    stored_count += 1
                else:
                    failed_count += 1
        else:
            # Use batch storage for better performance when fault tolerance is disabled
            try:
                cnx = DuckdbUtil.get_workspace_db_instance()
                
                # Prepare batch insert
                query = """
                    INSERT INTO pipeline_logs (
                        timestamp, pipeline_id, execution_id, log_level, logger_name,
                        message, module, function_name, line_number, thread_id,
                        process_id, correlation_id, extra_data, stack_trace, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # Prepare data for batch insert
                batch_data = []
                for record in log_records:
                    batch_data.append((
                        record.timestamp,
                        record.pipeline_id,
                        record.execution_id,
                        record.log_level,
                        record.logger_name,
                        record.message,
                        record.module,
                        record.function_name,
                        record.line_number,
                        record.thread_id,
                        record.process_id,
                        record.correlation_id,
                        record.extra_data_json,
                        record.stack_trace,
                        record.created_at
                    ))
                
                # Execute batch insert
                cnx.executemany(query, batch_data)
                stored_count = len(log_records)
                
                self.logger.debug(f"Successfully stored {stored_count} log records")
                
            except Exception as e:
                self.logger.error(f"Failed to store batch of {len(log_records)} log records: {e}")
                failed_count = len(log_records)
        
        return {
            'success': stored_count > 0,
            'stored_count': stored_count,
            'failed_count': failed_count,
            'total_count': len(log_records)
        }
    
    def query_logs(self, filters: LogQueryFilters) -> List[Dict[str, Any]]:
        """
        Query logs from storage with filters.
        
        Args:
            filters: Query filters and pagination
            
        Returns:
            List of log records matching the criteria
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            # Build the query dynamically based on filters
            where_clauses = []
            params = []
            
            if filters.pipeline_id:
                where_clauses.append("pipeline_id = ?")
                params.append(filters.pipeline_id)
            
            if filters.execution_id:
                where_clauses.append("execution_id = ?")
                params.append(filters.execution_id)
            
            if filters.log_levels:
                placeholders = ",".join("?" * len(filters.log_levels))
                where_clauses.append(f"log_level IN ({placeholders})")
                params.extend(filters.log_levels)
            
            if filters.start_time:
                where_clauses.append("timestamp >= ?")
                params.append(filters.start_time)
            
            if filters.end_time:
                where_clauses.append("timestamp <= ?")
                params.append(filters.end_time)
            
            if filters.logger_name:
                where_clauses.append("logger_name = ?")
                params.append(filters.logger_name)
            
            if filters.message_pattern:
                where_clauses.append("message LIKE ?")
                params.append(f"%{filters.message_pattern}%")
            
            if filters.correlation_id:
                where_clauses.append("correlation_id = ?")
                params.append(filters.correlation_id)
            
            # Build the complete query
            base_query = """
                SELECT id, timestamp, pipeline_id, execution_id, log_level,
                       logger_name, message, module, function_name, line_number,
                       thread_id, process_id, correlation_id, extra_data,
                       stack_trace, created_at
                FROM pipeline_logs
            """
            
            if where_clauses:
                base_query += " WHERE " + " AND ".join(where_clauses)
            
            # Add ordering (chronological by default)
            base_query += " ORDER BY timestamp DESC, id DESC"
            
            # Add pagination
            base_query += f" LIMIT {filters.limit} OFFSET {filters.offset}"
            
            # Execute query
            cursor = cnx.cursor()
            result = cursor.execute(base_query, params).fetchall()
            
            # Convert to list of dictionaries
            columns = [
                'id', 'timestamp', 'pipeline_id', 'execution_id', 'log_level',
                'logger_name', 'message', 'module', 'function_name', 'line_number',
                'thread_id', 'process_id', 'correlation_id', 'extra_data',
                'stack_trace', 'created_at'
            ]
            
            logs = []
            for row in result:
                log_dict = dict(zip(columns, row))
                
                # Parse JSON extra_data if present
                if log_dict['extra_data']:
                    try:
                        log_dict['extra_data'] = json.loads(log_dict['extra_data'])
                    except (json.JSONDecodeError, TypeError):
                        # If parsing fails, keep as string
                        pass
                
                logs.append(log_dict)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Failed to query logs: {e}")
            raise
    
    def get_pipeline_logs(self, pipeline_id: str, execution_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get all logs for a specific pipeline or execution.
        
        Args:
            pipeline_id: The pipeline identifier
            execution_id: Optional specific execution identifier
            
        Returns:
            List of log records for the pipeline/execution
        """
        filters = LogQueryFilters(
            pipeline_id=pipeline_id,
            execution_id=execution_id,
            limit=10000  # Large limit for pipeline-specific queries
        )
        
        return self.query_logs(filters)
    
    def get_recent_logs(self, hours: int = 24, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get recent logs within the specified time window.
        
        Args:
            hours: Number of hours to look back
            limit: Maximum number of logs to return
            
        Returns:
            List of recent log records
        """
        from datetime import timedelta
        
        start_time = datetime.now() - timedelta(hours=hours)
        
        filters = LogQueryFilters(
            start_time=start_time,
            limit=limit
        )
        
        return self.query_logs(filters)
    
    def get_error_logs(self, pipeline_id: Optional[str] = None, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get error and warning logs for debugging.
        
        Args:
            pipeline_id: Optional pipeline to filter by
            hours: Number of hours to look back
            
        Returns:
            List of error/warning log records
        """
        from datetime import timedelta
        
        start_time = datetime.now() - timedelta(hours=hours)
        
        filters = LogQueryFilters(
            pipeline_id=pipeline_id,
            log_levels=['ERROR', 'WARNING'],
            start_time=start_time,
            limit=5000
        )
        
        return self.query_logs(filters)
    
    def get_log_statistics(self, pipeline_id: Optional[str] = None, hours: int = 24) -> Dict[str, Any]:
        """
        Get log statistics for monitoring and analysis.
        
        Args:
            pipeline_id: Optional pipeline to filter by
            hours: Number of hours to analyze
            
        Returns:
            Dictionary with log statistics
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            from datetime import timedelta
            start_time = datetime.now() - timedelta(hours=hours)
            
            # Build base query with optional pipeline filter
            where_clause = "WHERE timestamp >= ?"
            params = [start_time]
            
            if pipeline_id:
                where_clause += " AND pipeline_id = ?"
                params.append(pipeline_id)
            
            # Get log counts by level
            level_query = f"""
                SELECT log_level, COUNT(*) as count
                FROM pipeline_logs
                {where_clause}
                GROUP BY log_level
                ORDER BY count DESC
            """
            
            level_stats = cnx.execute(level_query, params).fetchall()
            
            # Get total count
            total_query = f"""
                SELECT COUNT(*) as total_logs
                FROM pipeline_logs
                {where_clause}
            """
            
            total_count = cnx.execute(total_query, params).fetchone()[0]
            
            # Get unique pipelines count
            pipeline_query = f"""
                SELECT COUNT(DISTINCT pipeline_id) as unique_pipelines
                FROM pipeline_logs
                {where_clause}
            """
            
            unique_pipelines = cnx.execute(pipeline_query, params).fetchone()[0]
            
            return {
                'total_logs': total_count,
                'unique_pipelines': unique_pipelines,
                'log_levels': {level: count for level, count in level_stats},
                'time_window_hours': hours,
                'pipeline_id': pipeline_id
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get log statistics: {e}")
            return {
                'error': str(e),
                'total_logs': 0,
                'unique_pipelines': 0,
                'log_levels': {},
                'time_window_hours': hours,
                'pipeline_id': pipeline_id
            }
    
    def get_execution_timeline(self, execution_id: str) -> List[Dict[str, Any]]:
        """
        Get chronological timeline of logs for a specific execution.
        
        Args:
            execution_id: The execution identifier
            
        Returns:
            List of log records in chronological order
        """
        filters = LogQueryFilters(
            execution_id=execution_id,
            limit=50000  # Large limit for complete execution timeline
        )
        
        # Override the default ordering to be chronological (oldest first)
        logs = self.query_logs(filters)
        return list(reversed(logs))  # Reverse to get chronological order
    
    def search_logs_by_content(self, search_term: str, pipeline_id: Optional[str] = None, 
                              hours: int = 24, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Search logs by message content with optional pipeline filtering.
        
        Args:
            search_term: Text to search for in log messages
            pipeline_id: Optional pipeline to filter by
            hours: Number of hours to look back
            limit: Maximum number of results
            
        Returns:
            List of matching log records
        """
        from datetime import timedelta
        
        start_time = datetime.now() - timedelta(hours=hours)
        
        filters = LogQueryFilters(
            pipeline_id=pipeline_id,
            message_pattern=search_term,
            start_time=start_time,
            limit=limit
        )
        
        return self.query_logs(filters)
    
    def get_logs_by_correlation_id(self, correlation_id: str, limit: int = 10000) -> List[Dict[str, Any]]:
        """
        Get all logs sharing the same correlation ID for tracing related events.
        
        Args:
            correlation_id: The correlation identifier
            limit: Maximum number of logs to return
            
        Returns:
            List of related log records in chronological order
        """
        filters = LogQueryFilters(
            correlation_id=correlation_id,
            limit=limit
        )
        
        logs = self.query_logs(filters)
        return list(reversed(logs))  # Chronological order
    
    def get_pipeline_execution_summary(self, pipeline_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get summary of recent executions for a pipeline.
        
        Args:
            pipeline_id: The pipeline identifier
            limit: Maximum number of executions to return
            
        Returns:
            List of execution summaries with key metrics
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            # Get execution summaries with log counts and error status
            query = """
                SELECT 
                    execution_id,
                    MIN(timestamp) as start_time,
                    MAX(timestamp) as end_time,
                    COUNT(*) as total_logs,
                    SUM(CASE WHEN log_level = 'ERROR' THEN 1 ELSE 0 END) as error_count,
                    SUM(CASE WHEN log_level = 'WARNING' THEN 1 ELSE 0 END) as warning_count,
                    SUM(CASE WHEN log_level = 'INFO' THEN 1 ELSE 0 END) as info_count,
                    MAX(CASE WHEN log_level = 'ERROR' THEN message ELSE NULL END) as last_error
                FROM pipeline_logs 
                WHERE pipeline_id = ?
                GROUP BY execution_id
                ORDER BY start_time DESC
                LIMIT ?
            """
            
            result = cnx.execute(query, (pipeline_id, limit)).fetchall()
            
            columns = [
                'execution_id', 'start_time', 'end_time', 'total_logs',
                'error_count', 'warning_count', 'info_count', 'last_error'
            ]
            
            summaries = []
            for row in result:
                summary = dict(zip(columns, row))
                
                # Calculate duration if both start and end times exist
                if summary['start_time'] and summary['end_time']:
                    duration = summary['end_time'] - summary['start_time']
                    summary['duration_seconds'] = duration.total_seconds()
                else:
                    summary['duration_seconds'] = None
                
                # Determine execution status
                if summary['error_count'] > 0:
                    summary['status'] = 'failed'
                elif summary['warning_count'] > 0:
                    summary['status'] = 'warning'
                else:
                    summary['status'] = 'success'
                
                summaries.append(summary)
            
            return summaries
            
        except Exception as e:
            self.logger.error(f"Failed to get pipeline execution summary: {e}")
            return []
    
    def get_log_level_distribution(self, pipeline_id: Optional[str] = None, 
                                  hours: int = 24) -> Dict[str, int]:
        """
        Get distribution of log levels for analysis.
        
        Args:
            pipeline_id: Optional pipeline to filter by
            hours: Number of hours to analyze
            
        Returns:
            Dictionary mapping log levels to counts
        """
        stats = self.get_log_statistics(pipeline_id, hours)
        return stats.get('log_levels', {})
    
    def cleanup_old_logs(self, days_to_keep: int = 30, pipeline_id: Optional[str] = None) -> int:
        """
        Clean up old logs to manage storage growth with enhanced error handling.
        
        Args:
            days_to_keep: Number of days of logs to retain
            pipeline_id: Optional pipeline ID to clean logs for specific pipeline
            
        Returns:
            Number of logs deleted
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Build query with optional pipeline filter
            if pipeline_id:
                count_query = "SELECT COUNT(*) FROM pipeline_logs WHERE timestamp < ? AND pipeline_id = ?"
                delete_query = "DELETE FROM pipeline_logs WHERE timestamp < ? AND pipeline_id = ?"
                params = (cutoff_date, pipeline_id)
            else:
                count_query = "SELECT COUNT(*) FROM pipeline_logs WHERE timestamp < ?"
                delete_query = "DELETE FROM pipeline_logs WHERE timestamp < ?"
                params = (cutoff_date,)
            
            # Count logs to be deleted
            count_result = cnx.execute(count_query, params).fetchone()
            logs_to_delete = count_result[0] if count_result else 0
            
            if logs_to_delete > 0:
                # Delete old logs
                cnx.execute(delete_query, params)
                
                pipeline_info = f" for pipeline '{pipeline_id}'" if pipeline_id else ""
                self.logger.info(f"Cleaned up {logs_to_delete} old log records{pipeline_info} (older than {days_to_keep} days)")
            
            return logs_to_delete
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old logs: {e}")
            if self.enable_fault_tolerance:
                # Don't raise exception in fault-tolerant mode
                return 0
            else:
                raise
    
    def cleanup_by_count(self, max_logs_per_pipeline: int = 10000, 
                        pipeline_id: Optional[str] = None) -> int:
        """
        Clean up logs by keeping only the most recent N logs per pipeline.
        
        Args:
            max_logs_per_pipeline: Maximum number of logs to keep per pipeline
            pipeline_id: Optional pipeline ID to clean specific pipeline
            
        Returns:
            Number of logs deleted
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            deleted_count = 0
            
            # Get pipelines to clean
            if pipeline_id:
                pipelines = [pipeline_id]
            else:
                pipeline_result = cnx.execute("SELECT DISTINCT pipeline_id FROM pipeline_logs").fetchall()
                pipelines = [row[0] for row in pipeline_result]
            
            # Clean each pipeline
            for pid in pipelines:
                # Count logs for this pipeline
                count_result = cnx.execute(
                    "SELECT COUNT(*) FROM pipeline_logs WHERE pipeline_id = ?", 
                    (pid,)
                ).fetchone()
                
                log_count = count_result[0] if count_result else 0
                
                if log_count > max_logs_per_pipeline:
                    # Delete oldest logs, keeping only the most recent max_logs_per_pipeline
                    logs_to_delete = log_count - max_logs_per_pipeline
                    
                    delete_query = """
                        DELETE FROM pipeline_logs 
                        WHERE pipeline_id = ? 
                        AND id IN (
                            SELECT id FROM pipeline_logs 
                            WHERE pipeline_id = ? 
                            ORDER BY timestamp ASC, id ASC 
                            LIMIT ?
                        )
                    """
                    
                    cnx.execute(delete_query, (pid, pid, logs_to_delete))
                    deleted_count += logs_to_delete
                    
                    self.logger.info(f"Cleaned up {logs_to_delete} old logs for pipeline '{pid}' (kept {max_logs_per_pipeline} most recent)")
            
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup logs by count: {e}")
            if self.enable_fault_tolerance:
                return 0
            else:
                raise
    
    def get_retention_info(self) -> Dict[str, Any]:
        """
        Get information about log retention and storage usage.
        
        Returns:
            Dictionary with retention information
        """
        if not self._initialized:
            self.initialize_tables()
        
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            # Get total log count
            total_result = cnx.execute("SELECT COUNT(*) FROM pipeline_logs").fetchone()
            total_logs = total_result[0] if total_result else 0
            
            # Get oldest and newest log timestamps
            oldest_result = cnx.execute("SELECT MIN(timestamp) FROM pipeline_logs").fetchone()
            newest_result = cnx.execute("SELECT MAX(timestamp) FROM pipeline_logs").fetchone()
            
            oldest_log = oldest_result[0] if oldest_result and oldest_result[0] else None
            newest_log = newest_result[0] if newest_result and newest_result[0] else None
            
            # Get logs per pipeline
            pipeline_counts = cnx.execute("""
                SELECT pipeline_id, COUNT(*) as log_count, 
                       MIN(timestamp) as oldest, MAX(timestamp) as newest
                FROM pipeline_logs 
                GROUP BY pipeline_id 
                ORDER BY log_count DESC
            """).fetchall()
            
            # Calculate retention period
            retention_days = None
            if oldest_log and newest_log:
                retention_period = newest_log - oldest_log
                retention_days = retention_period.days
            
            return {
                'total_logs': total_logs,
                'oldest_log': oldest_log.isoformat() if oldest_log else None,
                'newest_log': newest_log.isoformat() if newest_log else None,
                'retention_days': retention_days,
                'pipelines': [
                    {
                        'pipeline_id': row[0],
                        'log_count': row[1],
                        'oldest': row[2].isoformat() if row[2] else None,
                        'newest': row[3].isoformat() if row[3] else None
                    }
                    for row in pipeline_counts
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get retention info: {e}")
            return {
                'error': str(e),
                'total_logs': 0,
                'retention_days': None,
                'pipelines': []
            }
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the log storage system.
        
        Returns:
            Dictionary with health status information
        """
        health_info = {
            'initialized': self._initialized,
            'fault_tolerance_enabled': self.enable_fault_tolerance,
            'primary_storage_available': True,
            'timestamp': datetime.now().isoformat()
        }
        
        # Test primary storage availability
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            cnx.execute("SELECT 1").fetchone()
        except Exception as e:
            health_info['primary_storage_available'] = False
            health_info['primary_storage_error'] = str(e)
        
        # Add fault tolerance status if enabled
        if self.enable_fault_tolerance and self.fault_tolerant_logger:
            health_info['fault_tolerance'] = self.fault_tolerant_logger.get_health_status()
        
        return health_info
    
    def recover_from_fallback(self) -> Dict[str, Any]:
        """
        Recover logs from fallback storage to primary storage.
        
        Returns:
            Dictionary with recovery results
        """
        if not self.enable_fault_tolerance or not self.fault_tolerant_logger:
            return {
                'success': False,
                'message': 'Fault tolerance not enabled',
                'recovered_count': 0
            }
        
        return self.fault_tolerant_logger.recover_from_fallback()
    
    def get_storage_statistics(self) -> Dict[str, Any]:
        """
        Get storage statistics and metrics.
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            'timestamp': datetime.now().isoformat(),
            'initialized': self._initialized,
            'fault_tolerance_enabled': self.enable_fault_tolerance
        }
        
        # Get database statistics
        try:
            cnx = DuckdbUtil.get_workspace_db_instance()
            
            # Get total log count
            result = cnx.execute("SELECT COUNT(*) FROM pipeline_logs").fetchone()
            stats['total_logs'] = result[0] if result else 0
            
            # Get logs by level
            level_counts = cnx.execute("""
                SELECT log_level, COUNT(*) 
                FROM pipeline_logs 
                GROUP BY log_level
            """).fetchall()
            stats['logs_by_level'] = {level: count for level, count in level_counts}
            
            # Get recent activity (last 24 hours)
            recent_count = cnx.execute("""
                SELECT COUNT(*) 
                FROM pipeline_logs 
                WHERE timestamp >= NOW() - INTERVAL '24 hours'
            """).fetchone()
            stats['logs_last_24h'] = recent_count[0] if recent_count else 0
            
        except Exception as e:
            stats['database_error'] = str(e)
        
        # Add fault tolerance statistics if enabled
        if self.enable_fault_tolerance and self.fault_tolerant_logger:
            ft_status = self.fault_tolerant_logger.get_health_status()
            stats['fault_tolerance_stats'] = ft_status.get('statistics', {})
        
        return stats


def create_log_storage_record(enriched_record: EnrichedLogRecord) -> LogStorageRecord:
    """
    Convert an EnrichedLogRecord to a LogStorageRecord for database storage.
    
    Args:
        enriched_record: The enriched log record
        
    Returns:
        Storage-ready log record
    """
    # Serialize extra_data to JSON if present
    extra_data_json = None
    if enriched_record.extra_data:
        try:
            extra_data_json = json.dumps(enriched_record.extra_data)
        except (TypeError, ValueError) as e:
            # If serialization fails, convert to string representation
            extra_data_json = json.dumps({'serialization_error': str(e), 'data': str(enriched_record.extra_data)})
    
    return LogStorageRecord(
        timestamp=enriched_record.timestamp,
        pipeline_id=enriched_record.pipeline_id,
        execution_id=enriched_record.execution_id,
        log_level=enriched_record.log_level,
        logger_name=enriched_record.logger_name,
        message=enriched_record.message,
        module=enriched_record.module,
        function_name=enriched_record.function_name,
        line_number=enriched_record.line_number,
        thread_id=enriched_record.thread_id,
        process_id=enriched_record.process_id,
        correlation_id=enriched_record.correlation_id,
        extra_data_json=extra_data_json,
        stack_trace=enriched_record.stack_trace,
        created_at=datetime.now()
    )