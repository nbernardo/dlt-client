"""
Logging Controller - Dedicated controller for log management endpoints.
Provides proper separation of concerns from pipeline operations.
"""

from flask import Blueprint, request, jsonify
from utils.log_storage import DuckDBLogStore
from utils.logging_models import LogQueryFilters
from utils.flask_logging_middleware import add_request_log_context
from datetime import datetime, timedelta
import traceback

logs = Blueprint('logs', __name__)


@logs.route('/logs/pipeline/<pipeline_id>', methods=['GET'])
def get_pipeline_logs(pipeline_id):
    """
    Get logs for a specific pipeline.
    
    Query parameters:
    - execution_id: Filter by execution ID
    - log_levels: Comma-separated list of log levels (DEBUG,INFO,WARNING,ERROR)
    - hours: Number of hours to look back (default: 24)
    - limit: Maximum number of logs to return (default: 1000)
    - offset: Number of logs to skip (default: 0)
    """
    try:
        # Add pipeline context to request logging
        add_request_log_context(pipeline_id=pipeline_id, operation="get_logs")
        
        # Parse query parameters
        execution_id = request.args.get('execution_id')
        log_levels_str = request.args.get('log_levels', '')
        hours = int(request.args.get('hours', 24))
        limit = int(request.args.get('limit', 1000))
        offset = int(request.args.get('offset', 0))
        
        # Parse log levels
        log_levels = []
        if log_levels_str:
            log_levels = [level.strip().upper() for level in log_levels_str.split(',')]
        
        # Create log store and query filters
        log_store = DuckDBLogStore()
        
        if execution_id:
            # Get logs for specific execution
            logs = log_store.get_pipeline_logs(pipeline_id, execution_id)
        else:
            # Get logs with filters
            filters = LogQueryFilters(
                pipeline_id=pipeline_id,
                log_levels=log_levels if log_levels else None,
                hours=hours,
                limit=limit,
                offset=offset
            )
            logs = log_store.query_logs(filters)
        
        # Convert datetime objects to ISO strings for JSON serialization
        for log in logs:
            if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
                log['timestamp'] = log['timestamp'].isoformat()
            if 'created_at' in log and hasattr(log['created_at'], 'isoformat'):
                log['created_at'] = log['created_at'].isoformat()
        
        return jsonify({
            'error': False,
            'result': {
                'logs': logs,
                'count': len(logs),
                'pipeline_id': pipeline_id,
                'execution_id': execution_id
            }
        })
        
    except Exception as e:
        print(f'Error retrieving pipeline logs: {str(e)}')
        traceback.print_exc()
        return jsonify({
            'error': True,
            'result': {'message': f'Error retrieving logs: {str(e)}'}
        }), 500


@logs.route('/logs/search', methods=['GET'])
def search_logs():
    """
    Search logs by content.
    
    Query parameters:
    - q: Search query string
    - pipeline_id: Filter by pipeline ID (optional)
    - log_levels: Comma-separated list of log levels (optional)
    - hours: Number of hours to look back (default: 24)
    - limit: Maximum number of logs to return (default: 100)
    """
    try:
        # Parse query parameters
        search_query = request.args.get('q', '').strip()
        pipeline_id = request.args.get('pipeline_id')
        log_levels_str = request.args.get('log_levels', '')
        hours = int(request.args.get('hours', 24))
        limit = int(request.args.get('limit', 100))
        
        if not search_query:
            return jsonify({
                'error': True,
                'result': {'message': 'Search query parameter "q" is required'}
            }), 400
        
        # Add search context to request logging
        add_request_log_context(search_query=search_query, operation="search_logs")
        
        # Parse log levels
        log_levels = []
        if log_levels_str:
            log_levels = [level.strip().upper() for level in log_levels_str.split(',')]
        
        # Create log store and search
        log_store = DuckDBLogStore()
        logs = log_store.search_logs_by_content(
            search_term=search_query,
            pipeline_id=pipeline_id,
            hours=hours,
            limit=limit
        )
        
        # Convert datetime objects to ISO strings for JSON serialization
        for log in logs:
            if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
                log['timestamp'] = log['timestamp'].isoformat()
            if 'created_at' in log and hasattr(log['created_at'], 'isoformat'):
                log['created_at'] = log['created_at'].isoformat()
        
        return jsonify({
            'error': False,
            'result': {
                'logs': logs,
                'count': len(logs),
                'search_query': search_query,
            }
        })
        
    except Exception as e:
        print(f'Error searching logs: {str(e)}')
        traceback.print_exc()
        return jsonify({
            'error': True,
            'result': {'message': f'Error searching logs: {str(e)}'}
        }), 500


@logs.route('/logs/correlation/<correlation_id>', methods=['GET'])
def get_logs_by_correlation(correlation_id):
    """
    Get all logs with a specific correlation ID.
    
    Query parameters:
    - limit: Maximum number of logs to return (default: 1000)
    """
    try:
        # Add correlation context to request logging
        add_request_log_context(correlation_id=correlation_id, operation="get_correlated_logs")
        
        limit = int(request.args.get('limit', 1000))
        
        # Create log store and query
        log_store = DuckDBLogStore()
        logs = log_store.get_logs_by_correlation_id(correlation_id, limit)
        
        # Convert datetime objects to ISO strings for JSON serialization
        for log in logs:
            if 'timestamp' in log and hasattr(log['timestamp'], 'isoformat'):
                log['timestamp'] = log['timestamp'].isoformat()
            if 'created_at' in log and hasattr(log['created_at'], 'isoformat'):
                log['created_at'] = log['created_at'].isoformat()
        
        return jsonify({
            'error': False,
            'result': {
                'logs': logs,
                'count': len(logs),
                'correlation_id': correlation_id
            }
        })
        
    except Exception as e:
        print(f'Error retrieving correlated logs: {str(e)}')
        traceback.print_exc()
        return jsonify({
            'error': True,
            'result': {'message': f'Error retrieving correlated logs: {str(e)}'}
        }), 500


@logs.route('/logs/stats', methods=['GET'])
def get_log_statistics():
    """
    Get logging statistics.
    
    Query parameters:
    - pipeline_id: Filter by pipeline ID (optional)
    - hours: Number of hours to look back (default: 24)
    """
    try:
        pipeline_id = request.args.get('pipeline_id')
        hours = int(request.args.get('hours', 24))
        
        # Add stats context to request logging
        add_request_log_context(operation="get_log_stats", hours=hours)
        
        # Create log store and get statistics
        log_store = DuckDBLogStore()
        stats = log_store.get_log_statistics(pipeline_id, hours)
        
        return jsonify({
            'error': False,
            'result': {
                'statistics': stats,
                'pipeline_id': pipeline_id,
                'hours': hours
            }
        })
        
    except Exception as e:
        print(f'Error retrieving log statistics: {str(e)}')
        traceback.print_exc()
        return jsonify({
            'error': True,
            'result': {'message': f'Error retrieving statistics: {str(e)}'}
        }), 500


@logs.route('/logs/cleanup', methods=['POST'])
def cleanup_old_logs():
    """
    Clean up old logs based on retention policy.
    
    JSON payload:
    - days: Number of days to retain (default: 30)
    - pipeline_id: Clean logs for specific pipeline (optional)
    """
    try:
        payload = request.get_json() or {}
        days = int(payload.get('days', 30))
        pipeline_id = payload.get('pipeline_id')
        
        # Add cleanup context to request logging
        add_request_log_context(operation="cleanup_logs", retention_days=days)
        
        # Create log store and perform cleanup
        log_store = DuckDBLogStore()
        deleted_count = log_store.cleanup_old_logs(days, pipeline_id)
        
        return jsonify({
            'error': False,
            'result': {
                'deleted_count': deleted_count,
                'retention_days': days,
                'pipeline_id': pipeline_id
            }
        })
        
    except Exception as e:
        print(f'Error cleaning up logs: {str(e)}')
        traceback.print_exc()
        return jsonify({
            'error': True,
            'result': {'message': f'Error cleaning up logs: {str(e)}'}
        }), 500