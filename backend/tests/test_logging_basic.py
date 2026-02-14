"""
Basic tests for the pipeline logging system.
Production-ready tests without external dependencies.
"""

import pytest
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Import the logging system components
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.logging_models import EnrichedLogRecord, LogQueryFilters, create_execution_id
from utils.log_storage import DuckDBLogStore, create_log_storage_record
from utils.logging_config import LoggingConfig


class TestLoggingModels:
    """Test core logging data models."""
    
    def test_create_execution_id(self):
        """Test execution ID generation."""
        exec_id = create_execution_id()
        assert exec_id.startswith('exec_')
        assert len(exec_id) == 17  # 'exec_' + 12 character hash
    
    def test_enriched_log_record_creation(self):
        """Test creating enriched log records."""
        record = EnrichedLogRecord(
            timestamp=datetime.now(),
            pipeline_id="test_pipeline",
            execution_id="exec_123",
            log_level="INFO",
            logger_name="test.logger",
            message="Test message",
            module="test_module",
            function_name="test_function",
            line_number=42
        )
        
        assert record.pipeline_id == "test_pipeline"
        assert record.log_level == "INFO"
        assert record.message == "Test message"
    
    def test_log_query_filters(self):
        """Test log query filter creation."""
        filters = LogQueryFilters(
            pipeline_id="test_pipeline",
            log_levels=["INFO", "ERROR"],
            hours=24,
            limit=100
        )
        
        assert filters.pipeline_id == "test_pipeline"
        assert filters.log_levels == ["INFO", "ERROR"]
        assert filters.limit == 100


class TestLogStorage:
    """Test log storage functionality."""
    
    @patch('utils.log_storage.DuckdbUtil')
    def test_log_store_initialization(self, mock_duckdb):
        """Test log store initialization."""
        mock_duckdb.initialize_logging_tables.return_value = None
        
        log_store = DuckDBLogStore(enable_fault_tolerance=False)
        log_store.initialize_tables()
        
        assert log_store._initialized is True
        mock_duckdb.initialize_logging_tables.assert_called_once()
    
    def test_create_log_storage_record(self):
        """Test converting enriched record to storage record."""
        enriched = EnrichedLogRecord(
            timestamp=datetime.now(),
            pipeline_id="test_pipeline",
            execution_id="exec_123",
            log_level="INFO",
            logger_name="test.logger",
            message="Test message",
            module="test_module",
            function_name="test_function",
            line_number=42,
            extra_data={"key": "value"}
        )
        
        storage_record = create_log_storage_record(enriched)
        
        assert storage_record.pipeline_id == "test_pipeline"
        assert storage_record.log_level == "INFO"
        assert storage_record.message == "Test message"
        assert '"key": "value"' in storage_record.extra_data_json


class TestLoggingConfig:
    """Test logging configuration."""
    
    def test_logging_config_defaults(self):
        """Test default logging configuration values."""
        config = LoggingConfig()
        
        assert config.log_level == "INFO"
        assert config.enable_fault_tolerance is True
        assert config.max_retry_attempts == 3
        assert config.enable_flask_logging is True
    
    def test_logging_config_from_env(self):
        """Test loading configuration from environment variables."""
        with patch.dict(os.environ, {
            'LOG_LEVEL': 'DEBUG',
            'ENABLE_FAULT_TOLERANCE': 'false',
            'MAX_RETRY_ATTEMPTS': '5'
        }):
            config = LoggingConfig.from_environment()
            
            assert config.log_level == "DEBUG"
            assert config.enable_fault_tolerance is False
            assert config.max_retry_attempts == 5


class TestIntegration:
    """Integration tests for the logging system."""
    
    @patch('utils.log_storage.DuckdbUtil')
    def test_end_to_end_logging_flow(self, mock_duckdb):
        """Test complete logging flow from record creation to storage."""
        # Mock database connection
        mock_connection = MagicMock()
        mock_duckdb.get_workspace_db_instance.return_value = mock_connection
        mock_duckdb.initialize_logging_tables.return_value = None
        
        # Create log store
        log_store = DuckDBLogStore(enable_fault_tolerance=False)
        log_store.initialize_tables()
        
        # Create enriched log record
        enriched = EnrichedLogRecord(
            timestamp=datetime.now(),
            pipeline_id="integration_test_pipeline",
            execution_id=create_execution_id(),
            log_level="INFO",
            logger_name="integration.test",
            message="Integration test message",
            module="test_integration",
            function_name="test_end_to_end_logging_flow",
            line_number=100
        )
        
        # Convert to storage record
        storage_record = create_log_storage_record(enriched)
        
        # Store the record
        result = log_store.store_log(storage_record)
        
        # Verify the flow completed
        assert result is True
        mock_connection.execute.assert_called_once()


if __name__ == "__main__":
    # Run basic tests
    pytest.main([__file__, "-v"])