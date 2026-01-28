"""
Log processing components for the pipeline logging enhancement system.

This module provides log processing, enrichment, and coordination between
real-time WebSocket logging and persistent DuckDB storage with comprehensive
error handling and fault tolerance.
"""

import json
import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any
import threading
from queue import Queue, Empty

from .logging_models import (
    EnrichedLogRecord, LogStorageRecord, PipelineContext,
    normalize_log_level, extract_extra_data
)
from .log_storage import DuckDBLogStore, create_log_storage_record
from .fault_tolerance import LoggingError


class LogProcessor:
    """
    Processes and enriches log records before storage with fault tolerance.
    
    Coordinates between real-time WebSocket logging and persistent DuckDB storage,
    ensuring both systems receive log data without interfering with each other.
    Includes comprehensive error handling to prevent logging failures from
    disrupting pipeline execution.
    """
    
    def __init__(self, storage: DuckDBLogStore, websocket_logger=None, enable_graceful_degradation: bool = True):
        """
        Initialize the log processor.
        
        Args:
            storage: DuckDB storage backend
            websocket_logger: Optional WebSocket logger for real-time updates
            enable_graceful_degradation: Whether to continue operation if storage fails
        """
        self.storage = storage
        self.websocket_logger = websocket_logger
        self.enable_graceful_degradation = enable_graceful_degradation
        self.logger = logging.getLogger(__name__)
        
        # Thread-safe processing queue for batch operations
        self._processing_queue = Queue()
        self._processing_thread = None
        self._stop_processing = threading.Event()
        self._batch_size = 10
        self._batch_timeout = 5.0  # seconds
        
        # Error tracking for graceful degradation
        self._consecutive_errors = 0
        self._max_consecutive_errors = 5
        self._storage_disabled = False
        self._last_error_time: Optional[datetime] = None
        self._error_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'processed_count': 0,
            'storage_success_count': 0,
            'storage_error_count': 0,
            'websocket_success_count': 0,
            'websocket_error_count': 0,
            'degraded_mode_activations': 0
        }
        self.stats_lock = threading.Lock()
        
        # Start background processing thread
        self._start_background_processing()
    
    def _start_background_processing(self):
        """Start the background thread for batch log processing."""
        if self._processing_thread is None or not self._processing_thread.is_alive():
            self._stop_processing.clear()
            self._processing_thread = threading.Thread(
                target=self._background_processor,
                daemon=True,
                name="LogProcessor"
            )
            self._processing_thread.start()
    
    def _background_processor(self):
        """Background thread that processes logs in batches for efficiency."""
        batch = []
        last_flush = datetime.now()
        
        while not self._stop_processing.is_set():
            try:
                # Try to get a log record with timeout
                try:
                    log_record = self._processing_queue.get(timeout=1.0)
                    
                    # Check if this is a flush marker
                    if hasattr(log_record, 'event') and hasattr(log_record, '__class__') and 'FlushMarker' in str(log_record.__class__):
                        # This is a flush marker - flush current batch and signal completion
                        if batch:
                            try:
                                self.storage.store_logs_batch(batch)
                                self.logger.debug(f"Flushed batch of {len(batch)} log records")
                                batch.clear()
                                last_flush = datetime.now()
                            except Exception as e:
                                self.logger.error(f"Failed to flush log batch: {e}")
                                batch.clear()
                        
                        # Signal flush completion
                        log_record.event.set()
                        continue
                    
                    # Regular log record - add to batch
                    batch.append(log_record)
                    
                except Empty:
                    # No new logs, check if we should flush existing batch
                    pass
                
                # Check if we should flush the batch
                now = datetime.now()
                should_flush = (
                    len(batch) >= self._batch_size or
                    (batch and (now - last_flush).total_seconds() >= self._batch_timeout)
                )
                
                if should_flush and batch:
                    try:
                        # Store batch to database
                        self.storage.store_logs_batch(batch)
                        self.logger.debug(f"Processed batch of {len(batch)} log records")
                        batch.clear()
                        last_flush = now
                    except Exception as e:
                        self.logger.error(f"Failed to store log batch: {e}")
                        # Clear the batch to prevent infinite retry
                        batch.clear()
                
            except Exception as e:
                self.logger.error(f"Error in background log processor: {e}")
                # Clear batch on error to prevent corruption
                batch.clear()
    
    def stop(self):
        """Stop the background processing thread."""
        self._stop_processing.set()
        if self._processing_thread and self._processing_thread.is_alive():
            self._processing_thread.join(timeout=5.0)
    
    def process_log(self, record: logging.LogRecord, context: PipelineContext) -> None:
        """
        Process a log record with pipeline context and fault tolerance.
        
        Args:
            record: The original log record
            context: Pipeline execution context
        """
        with self.stats_lock:
            self.stats['processed_count'] += 1
        
        try:
            # Enrich the log record
            enriched_record = self.enrich_record(record, context)
            
            # Send to WebSocket for real-time display (non-blocking)
            if self.websocket_logger:
                try:
                    self._send_to_websocket(enriched_record, record)
                    with self.stats_lock:
                        self.stats['websocket_success_count'] += 1
                except Exception as e:
                    with self.stats_lock:
                        self.stats['websocket_error_count'] += 1
                    self.logger.debug(f"WebSocket logging failed (non-critical): {e}")
            
            # Format for storage
            storage_record = self.format_for_storage(enriched_record)
            
            # Check if storage is disabled due to consecutive errors
            if self._storage_disabled and self.enable_graceful_degradation:
                self.logger.debug("Storage disabled due to consecutive errors, skipping storage")
                return
            
            # Add to processing queue for batch storage
            try:
                self._processing_queue.put_nowait(storage_record)
                self._on_storage_success()
            except Exception as e:
                self._on_storage_error(e)
                self.logger.error(f"Failed to queue log for storage: {e}")
                
                # Try immediate storage as fallback if not in degraded mode
                if not self._storage_disabled:
                    try:
                        success = self.storage.store_log(storage_record)
                        if success:
                            self._on_storage_success()
                        else:
                            self._on_storage_error(Exception("Storage returned False"))
                    except Exception as storage_error:
                        self._on_storage_error(storage_error)
                        self.logger.error(f"Failed to store log immediately: {storage_error}")
        
        except Exception as e:
            self.logger.error(f"Failed to process log record: {e}")
            # Don't raise the exception to prevent disrupting pipeline execution
    
    def _on_storage_success(self):
        """Handle successful storage operation."""
        with self.stats_lock:
            self.stats['storage_success_count'] += 1
        
        with self._error_lock:
            # Reset error count on success
            if self._consecutive_errors > 0:
                self.logger.info(f"Storage recovered after {self._consecutive_errors} consecutive errors")
                self._consecutive_errors = 0
            
            # Re-enable storage if it was disabled
            if self._storage_disabled:
                self._storage_disabled = False
                self.logger.info("Storage re-enabled after successful operation")
    
    def _on_storage_error(self, error: Exception):
        """Handle storage error with graceful degradation."""
        with self.stats_lock:
            self.stats['storage_error_count'] += 1
        
        with self._error_lock:
            self._consecutive_errors += 1
            self._last_error_time = datetime.now()
            
            # Check if we should disable storage
            if (self.enable_graceful_degradation and 
                self._consecutive_errors >= self._max_consecutive_errors and 
                not self._storage_disabled):
                
                self._storage_disabled = True
                with self.stats_lock:
                    self.stats['degraded_mode_activations'] += 1
                
                self.logger.error(
                    f"Storage disabled after {self._consecutive_errors} consecutive errors. "
                    f"Last error: {error}. Pipeline execution will continue with WebSocket logging only."
                )
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get health status of the log processor.
        
        Returns:
            Dictionary with health status information
        """
        with self.stats_lock:
            stats_copy = self.stats.copy()
        
        with self._error_lock:
            error_info = {
                'consecutive_errors': self._consecutive_errors,
                'storage_disabled': self._storage_disabled,
                'last_error_time': self._last_error_time.isoformat() if self._last_error_time else None
            }
        
        return {
            'statistics': stats_copy,
            'error_tracking': error_info,
            'queue_size': self._processing_queue.qsize(),
            'processing_thread_alive': self._processing_thread.is_alive() if self._processing_thread else False,
            'graceful_degradation_enabled': self.enable_graceful_degradation,
            'health': 'healthy' if not self._storage_disabled else 'degraded'
        }
    
    def reset_error_state(self) -> bool:
        """
        Reset error state and re-enable storage.
        
        Returns:
            True if reset was successful, False otherwise
        """
        try:
            with self._error_lock:
                self._consecutive_errors = 0
                self._storage_disabled = False
                self._last_error_time = None
            
            self.logger.info("Error state reset, storage re-enabled")
            return True
        except Exception as e:
            self.logger.error(f"Failed to reset error state: {e}")
            return False
    
    def enrich_record(self, record: logging.LogRecord, context: PipelineContext) -> EnrichedLogRecord:
        """
        Enrich a log record with context and metadata.
        
        Args:
            record: The original log record
            context: Pipeline execution context
            
        Returns:
            Enriched log record with full context
        """
        # Extract basic information from the log record
        timestamp = datetime.fromtimestamp(record.created)
        log_level = normalize_log_level(record.levelname)
        
        # Extract stack trace if there's an exception
        stack_trace = None
        if record.exc_info:
            stack_trace = ''.join(traceback.format_exception(*record.exc_info))
        elif hasattr(record, 'stack_info') and record.stack_info:
            stack_trace = record.stack_info
        
        # Extract extra data from the record
        extra_data = extract_extra_data(record)
        
        # Add pipeline context to extra data
        if extra_data is None:
            extra_data = {}
        
        extra_data.update({
            'namespace': context.namespace,
            'user_id': context.user_id,
            'pipeline_name': context.pipeline_name,
            'template_type': context.template_type,
            'execution_start_time': context.start_time.isoformat() if context.start_time else None
        })
        
        # Create enriched record
        # Use execution_id as the correlation_id to ensure consistency across all logs in the same execution
        correlation_id = getattr(record, 'correlation_id', None) or context.execution_id
        
        enriched = EnrichedLogRecord(
            timestamp=timestamp,
            pipeline_id=context.pipeline_id,
            execution_id=context.execution_id,
            log_level=log_level,
            logger_name=record.name,
            message=record.getMessage(),
            module=getattr(record, 'module', None) or record.filename,
            function_name=record.funcName,
            line_number=record.lineno,
            thread_id=record.thread,
            process_id=record.process,
            correlation_id=correlation_id,
            extra_data=extra_data,
            stack_trace=stack_trace
        )
        
        return enriched
    
    def format_for_storage(self, enriched_record: EnrichedLogRecord) -> LogStorageRecord:
        """
        Format an enriched record for database storage.
        
        Args:
            enriched_record: The enriched log record
            
        Returns:
            Storage-ready log record
        """
        return create_log_storage_record(enriched_record)
    
    def _send_to_websocket(self, enriched_record: EnrichedLogRecord, original_record: logging.LogRecord):
        """
        Send log to WebSocket for real-time display.
        
        Args:
            enriched_record: The enriched log record
            original_record: The original log record
        """
        if not self.websocket_logger:
            return
        
        try:
            # Format message for WebSocket (similar to current format)
            websocket_message = {
                'timestamp': enriched_record.timestamp.isoformat(),
                'level': enriched_record.log_level,
                'logger': enriched_record.logger_name,
                'message': enriched_record.message,
                'pipeline_id': enriched_record.pipeline_id,
                'execution_id': enriched_record.execution_id,
                'correlation_id': enriched_record.correlation_id
            }
            
            # Add stack trace if present
            if enriched_record.stack_trace:
                websocket_message['stack_trace'] = enriched_record.stack_trace
            
            # Send to WebSocket logger
            # Note: The actual WebSocket implementation will depend on the existing system
            # This is a placeholder for the integration point
            self.websocket_logger.emit_log(websocket_message)
            
        except Exception as e:
            # WebSocket errors should not disrupt log processing
            self.logger.debug(f"WebSocket emission failed: {e}")
    
    def process_immediate(self, record: logging.LogRecord, context: PipelineContext) -> None:
        """
        Process a log record immediately without batching (for critical logs).
        
        Args:
            record: The original log record
            context: Pipeline execution context
        """
        try:
            # Enrich and format the record
            enriched_record = self.enrich_record(record, context)
            storage_record = self.format_for_storage(enriched_record)
            
            # Store immediately
            self.storage.store_log(storage_record)
            
            # Send to WebSocket
            if self.websocket_logger:
                try:
                    self._send_to_websocket(enriched_record, record)
                except Exception as e:
                    self.logger.debug(f"WebSocket logging failed: {e}")
        
        except Exception as e:
            self.logger.error(f"Failed to process immediate log: {e}")
    
    def flush(self):
        """
        Flush any pending logs to storage immediately.
        
        This method blocks until all queued logs are processed.
        """
        # Signal the background processor to flush
        # We'll add a special flush marker to the queue
        flush_event = threading.Event()
        
        class FlushMarker:
            def __init__(self, event):
                self.event = event
        
        try:
            self._processing_queue.put_nowait(FlushMarker(flush_event))
            # Wait for flush to complete (with timeout)
            flush_event.wait(timeout=10.0)
        except Exception as e:
            self.logger.error(f"Failed to flush logs: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get processing statistics for monitoring.
        
        Returns:
            Dictionary with processing statistics
        """
        with self.stats_lock:
            stats_copy = self.stats.copy()
        
        return {
            'statistics': stats_copy,
            'queue_size': self._processing_queue.qsize(),
            'processing_thread_alive': self._processing_thread.is_alive() if self._processing_thread else False,
            'batch_size': self._batch_size,
            'batch_timeout': self._batch_timeout,
            'storage_disabled': self._storage_disabled,
            'consecutive_errors': self._consecutive_errors
        }


class WebSocketLoggerAdapter:
    """
    Adapter for integrating with the existing WebSocket logging system.
    
    This class provides a bridge between the new logging system and the
    existing real-time WebSocket logging functionality.
    """
    
    def __init__(self, socketio_instance=None, namespace='/pipeline'):
        """
        Initialize the WebSocket logger adapter.
        
        Args:
            socketio_instance: The Flask-SocketIO instance
            namespace: WebSocket namespace for pipeline logs
        """
        self.socketio = socketio_instance
        self.namespace = namespace
        self.logger = logging.getLogger(__name__)
    
    def emit_log(self, log_data: Dict[str, Any]):
        """
        Emit a log message to connected WebSocket clients.
        
        Args:
            log_data: Dictionary containing log information
        """
        if not self.socketio:
            return
        
        try:
            # Emit to all connected clients in the pipeline namespace
            self.socketio.emit('log_message', log_data, namespace=self.namespace)
        except Exception as e:
            self.logger.debug(f"Failed to emit WebSocket log: {e}")
    
    def emit_log_to_session(self, session_id: str, log_data: Dict[str, Any]):
        """
        Emit a log message to a specific WebSocket session.
        
        Args:
            session_id: The WebSocket session ID
            log_data: Dictionary containing log information
        """
        if not self.socketio:
            return
        
        try:
            # Emit to specific session
            self.socketio.emit('log_message', log_data, room=session_id, namespace=self.namespace)
        except Exception as e:
            self.logger.debug(f"Failed to emit WebSocket log to session {session_id}: {e}")


# Global log processor instance (will be initialized by the application)
_global_log_processor: Optional[LogProcessor] = None


def get_log_processor() -> Optional[LogProcessor]:
    """Get the global log processor instance."""
    return _global_log_processor


def initialize_log_processor(storage: DuckDBLogStore, websocket_logger=None) -> LogProcessor:
    """
    Initialize the global log processor.
    
    Args:
        storage: DuckDB storage backend
        websocket_logger: Optional WebSocket logger
        
    Returns:
        The initialized log processor
    """
    global _global_log_processor
    
    if _global_log_processor:
        _global_log_processor.stop()
    
    _global_log_processor = LogProcessor(storage, websocket_logger)
    return _global_log_processor


def shutdown_log_processor():
    """Shutdown the global log processor."""
    global _global_log_processor
    
    if _global_log_processor:
        _global_log_processor.stop()
        _global_log_processor = None