"""
Fault tolerance and error handling utilities for the pipeline logging system.

This module provides comprehensive error handling, retry logic, circuit breaker
patterns, and graceful degradation capabilities to ensure pipeline execution
is never disrupted by logging failures.
"""

import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Optional, Callable, Any, Dict, List
from dataclasses import dataclass, field
from enum import Enum
import json
import tempfile
import os
from pathlib import Path


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff multiplier
    jitter: bool = True  # Add random jitter to prevent thundering herd


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: int = 60  # Seconds before attempting recovery
    success_threshold: int = 3  # Successful calls needed to close circuit


@dataclass
class FallbackStorageConfig:
    """Configuration for fallback storage."""
    enabled: bool = True
    temp_dir: Optional[str] = None  # Use system temp if None
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    max_files: int = 100
    cleanup_interval: int = 3600  # Cleanup every hour


class LoggingError(Exception):
    """Base exception for logging system errors."""
    pass


class StorageUnavailableError(LoggingError):
    """Raised when primary storage is unavailable."""
    pass


class CircuitBreakerOpenError(LoggingError):
    """Raised when circuit breaker is open."""
    pass


class RetryExhaustedError(LoggingError):
    """Raised when all retry attempts are exhausted."""
    pass


class RetryHandler:
    """
    Implements exponential backoff retry logic with jitter.
    """
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with retry logic.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            RetryExhaustedError: If all retry attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.config.max_attempts - 1:
                    # Last attempt failed
                    break
                
                # Calculate delay with exponential backoff
                delay = min(
                    self.config.base_delay * (self.config.exponential_base ** attempt),
                    self.config.max_delay
                )
                
                # Add jitter to prevent thundering herd
                if self.config.jitter:
                    import random
                    delay *= (0.5 + random.random() * 0.5)
                
                self.logger.warning(
                    f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s: {e}"
                )
                time.sleep(delay)
        
        # All attempts failed
        raise RetryExhaustedError(
            f"All {self.config.max_attempts} retry attempts failed. Last error: {last_exception}"
        ) from last_exception


class CircuitBreaker:
    """
    Implements circuit breaker pattern to prevent cascading failures.
    """
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function through the circuit breaker.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerOpenError: If circuit is open
        """
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        
        time_since_failure = datetime.now() - self.last_failure_time
        return time_since_failure.total_seconds() >= self.config.recovery_timeout
    
    def _on_success(self):
        """Handle successful call."""
        with self.lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.logger.info("Circuit breaker CLOSED - service recovered")
            elif self.state == CircuitBreakerState.CLOSED:
                # Reset failure count on successful call
                self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed call."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                # Failed during recovery attempt
                self.state = CircuitBreakerState.OPEN
                self.success_count = 0
                self.logger.warning("Circuit breaker OPEN - recovery attempt failed")
            elif (self.state == CircuitBreakerState.CLOSED and 
                  self.failure_count >= self.config.failure_threshold):
                # Too many failures, open the circuit
                self.state = CircuitBreakerState.OPEN
                self.logger.error(
                    f"Circuit breaker OPEN - {self.failure_count} failures exceeded threshold"
                )
    
    def get_state(self) -> Dict[str, Any]:
        """Get current circuit breaker state information."""
        with self.lock:
            return {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'success_count': self.success_count,
                'last_failure_time': self.last_failure_time.isoformat() if self.last_failure_time else None
            }


class FallbackStorage:
    """
    Implements fallback file-based storage when primary storage is unavailable.
    """
    
    def __init__(self, config: FallbackStorageConfig):
        self.config = config
        self.temp_dir = Path(config.temp_dir or tempfile.gettempdir()) / "pipeline_logs_fallback"
        self.temp_dir.mkdir(exist_ok=True)
        self.current_file: Optional[Path] = None
        self.current_file_size = 0
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
        # Start cleanup thread
        self._start_cleanup_thread()
    
    def store_log(self, log_data: Dict[str, Any]) -> None:
        """
        Store log data to fallback storage.
        
        Args:
            log_data: Log data dictionary
        """
        if not self.config.enabled:
            return
        
        with self.lock:
            # Check if we need a new file
            if (self.current_file is None or 
                self.current_file_size >= self.config.max_file_size):
                self._create_new_file()
            
            # Write log entry
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'data': log_data
            }
            
            try:
                with open(self.current_file, 'a', encoding='utf-8') as f:
                    json.dump(log_entry, f)
                    f.write('\n')
                    self.current_file_size += len(json.dumps(log_entry)) + 1
                
                self.logger.debug(f"Stored log to fallback storage: {self.current_file}")
            except Exception as e:
                self.logger.error(f"Failed to write to fallback storage: {e}")
    
    def _create_new_file(self) -> None:
        """Create a new fallback storage file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        self.current_file = self.temp_dir / f"logs_{timestamp}.jsonl"
        self.current_file_size = 0
        
        self.logger.info(f"Created new fallback storage file: {self.current_file}")
    
    def get_stored_logs(self) -> List[Dict[str, Any]]:
        """
        Retrieve all logs from fallback storage.
        
        Returns:
            List of stored log entries
        """
        logs = []
        
        try:
            for file_path in self.temp_dir.glob("logs_*.jsonl"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                log_entry = json.loads(line)
                                logs.append(log_entry)
                except Exception as e:
                    self.logger.error(f"Failed to read fallback file {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to scan fallback directory: {e}")
        
        return logs
    
    def clear_stored_logs(self) -> int:
        """
        Clear all stored logs from fallback storage.
        
        Returns:
            Number of files cleared
        """
        cleared_count = 0
        
        try:
            for file_path in self.temp_dir.glob("logs_*.jsonl"):
                try:
                    file_path.unlink()
                    cleared_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to delete fallback file {file_path}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to clear fallback directory: {e}")
        
        with self.lock:
            self.current_file = None
            self.current_file_size = 0
        
        self.logger.info(f"Cleared {cleared_count} fallback storage files")
        return cleared_count
    
    def _start_cleanup_thread(self) -> None:
        """Start background thread for periodic cleanup."""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(self.config.cleanup_interval)
                    self._cleanup_old_files()
                except Exception as e:
                    self.logger.error(f"Cleanup thread error: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
    
    def _cleanup_old_files(self) -> None:
        """Clean up old fallback files if we exceed max_files."""
        try:
            files = list(self.temp_dir.glob("logs_*.jsonl"))
            
            if len(files) <= self.config.max_files:
                return
            
            # Sort by modification time (oldest first)
            files.sort(key=lambda f: f.stat().st_mtime)
            
            # Remove oldest files
            files_to_remove = files[:-self.config.max_files]
            for file_path in files_to_remove:
                try:
                    file_path.unlink()
                    self.logger.debug(f"Cleaned up old fallback file: {file_path}")
                except Exception as e:
                    self.logger.error(f"Failed to cleanup file {file_path}: {e}")
        
        except Exception as e:
            self.logger.error(f"Cleanup operation failed: {e}")


class FaultTolerantLogger:
    """
    Main fault-tolerant logging coordinator that combines all resilience patterns.
    """
    
    def __init__(self, 
                 primary_storage_func: Callable,
                 retry_config: Optional[RetryConfig] = None,
                 circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
                 fallback_config: Optional[FallbackStorageConfig] = None):
        """
        Initialize fault-tolerant logger.
        
        Args:
            primary_storage_func: Function to call for primary storage
            retry_config: Retry configuration
            circuit_breaker_config: Circuit breaker configuration
            fallback_config: Fallback storage configuration
        """
        self.primary_storage_func = primary_storage_func
        self.retry_handler = RetryHandler(retry_config or RetryConfig())
        self.circuit_breaker = CircuitBreaker(circuit_breaker_config or CircuitBreakerConfig())
        self.fallback_storage = FallbackStorage(fallback_config or FallbackStorageConfig())
        self.logger = logging.getLogger(__name__)
        
        # Statistics
        self.stats = {
            'total_attempts': 0,
            'successful_primary': 0,
            'fallback_used': 0,
            'circuit_breaker_trips': 0,
            'retry_attempts': 0
        }
        self.stats_lock = threading.Lock()
    
    def store_log(self, log_record) -> bool:
        """
        Store log record with full fault tolerance.
        
        Args:
            log_record: Log record to store
            
        Returns:
            True if stored successfully (primary or fallback), False otherwise
        """
        with self.stats_lock:
            self.stats['total_attempts'] += 1
        
        try:
            # Try primary storage with circuit breaker and retry
            def primary_storage():
                return self.primary_storage_func(log_record)
            
            # Execute through circuit breaker and retry handler
            self.circuit_breaker.call(
                self.retry_handler.execute_with_retry,
                primary_storage
            )
            
            with self.stats_lock:
                self.stats['successful_primary'] += 1
            
            return True
            
        except CircuitBreakerOpenError:
            # Circuit breaker is open, use fallback immediately
            with self.stats_lock:
                self.stats['circuit_breaker_trips'] += 1
            
            self.logger.warning("Circuit breaker open, using fallback storage")
            return self._use_fallback_storage(log_record)
            
        except RetryExhaustedError as e:
            # All retries failed, use fallback
            with self.stats_lock:
                self.stats['retry_attempts'] += self.retry_handler.config.max_attempts
            
            self.logger.error(f"Primary storage failed after retries: {e}")
            return self._use_fallback_storage(log_record)
            
        except Exception as e:
            # Unexpected error, use fallback
            self.logger.error(f"Unexpected error in primary storage: {e}")
            return self._use_fallback_storage(log_record)
    
    def _use_fallback_storage(self, log_record) -> bool:
        """
        Store log record in fallback storage.
        
        Args:
            log_record: Log record to store
            
        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # Convert log record to dictionary for fallback storage
            log_data = {
                'timestamp': log_record.timestamp.isoformat() if hasattr(log_record.timestamp, 'isoformat') else str(log_record.timestamp),
                'pipeline_id': log_record.pipeline_id,
                'execution_id': log_record.execution_id,
                'log_level': log_record.log_level,
                'logger_name': log_record.logger_name,
                'message': log_record.message,
                'module': log_record.module,
                'function_name': log_record.function_name,
                'line_number': log_record.line_number,
                'thread_id': log_record.thread_id,
                'process_id': log_record.process_id,
                'correlation_id': log_record.correlation_id,
                'extra_data_json': getattr(log_record, 'extra_data_json', None),
                'stack_trace': log_record.stack_trace,
                'created_at': getattr(log_record, 'created_at', datetime.now()).isoformat()
            }
            
            self.fallback_storage.store_log(log_data)
            
            with self.stats_lock:
                self.stats['fallback_used'] += 1
            
            return True
            
        except Exception as e:
            self.logger.error(f"Fallback storage also failed: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the fault-tolerant logger.
        
        Returns:
            Health status information
        """
        with self.stats_lock:
            stats_copy = self.stats.copy()
        
        return {
            'circuit_breaker': self.circuit_breaker.get_state(),
            'statistics': stats_copy,
            'fallback_storage_enabled': self.fallback_storage.config.enabled,
            'health': 'healthy' if self.circuit_breaker.state == CircuitBreakerState.CLOSED else 'degraded'
        }
    
    def recover_from_fallback(self) -> Dict[str, Any]:
        """
        Attempt to recover logs from fallback storage to primary storage.
        
        Returns:
            Recovery status information
        """
        if self.circuit_breaker.state == CircuitBreakerState.OPEN:
            return {
                'success': False,
                'message': 'Cannot recover while circuit breaker is open',
                'recovered_count': 0
            }
        
        try:
            fallback_logs = self.fallback_storage.get_stored_logs()
            recovered_count = 0
            failed_count = 0
            
            for log_entry in fallback_logs:
                try:
                    # Convert back to log record format and store
                    log_data = log_entry['data']
                    
                    # Create a simple object with the required attributes
                    class LogRecord:
                        pass
                    
                    record = LogRecord()
                    for key, value in log_data.items():
                        setattr(record, key, value)
                    
                    # Convert timestamp back to datetime if it's a string
                    if isinstance(record.timestamp, str):
                        record.timestamp = datetime.fromisoformat(record.timestamp)
                    
                    # Try to store in primary storage (without retry/circuit breaker to avoid recursion)
                    self.primary_storage_func(record)
                    recovered_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"Failed to recover log entry: {e}")
            
            # Clear fallback storage if recovery was successful
            if recovered_count > 0 and failed_count == 0:
                self.fallback_storage.clear_stored_logs()
            
            return {
                'success': True,
                'message': f'Recovered {recovered_count} logs, {failed_count} failed',
                'recovered_count': recovered_count,
                'failed_count': failed_count
            }
            
        except Exception as e:
            self.logger.error(f"Recovery operation failed: {e}")
            return {
                'success': False,
                'message': f'Recovery failed: {e}',
                'recovered_count': 0
            }