"""
Logging Controller - Dedicated controller for log management endpoints.
Provides proper separation of concerns from pipeline operations.
"""


from flask import Flask, jsonify, request
from utils.logging.log_storage import DuckDBLogStore

logs = Flask(__name__)
store = DuckDBLogStore()

@logs.route('/health/pivot', methods=['GET'])
def get_system_health():
    """Side-by-side comparison of log levels per pipeline."""
    df = store.get_health_pivot()
    return jsonify(df.to_dict(orient="records"))


@logs.route('/health/stuck', methods=['GET'])
def get_stuck_jobs():
    """Finds pipelines that started but never finished. ?timeout=1"""
    timeout = request.args.get('timeout', default=1, type=int)
    df = store.get_stuck_pipelines(timeout_hours=timeout)
    return jsonify(df.to_dict(orient="records"))


@logs.route('/metrics/performance', methods=['GET'])
def get_performance():
    """Throughput and duration metrics extracted from extra_data."""
    df = store.get_performance_metrics()
    return jsonify(df.to_dict(orient="records"))


@logs.route('/metrics/errors', methods=['GET'])
def get_error_hotspots():
    """Identifies the 'noisiest' modules in the system."""
    df = store.get_error_hotspots()
    return jsonify(df.to_dict(orient="records"))


@logs.route('/timeline/<execution_id>', methods=['GET'])
def get_run_timeline(execution_id):
    """Detailed step-by-step lag analysis for a specific execution."""
    df = store.get_execution_timeline(execution_id)
    if df.empty:
        return jsonify({"error": "Execution ID not found"}), 404
    return jsonify(df.to_dict(orient="records"))


@logs.route('/logs/volume', methods=['GET'])
def get_volume():
    """Histogram of log frequency. ?interval=5 minutes&hours=6"""
    interval = request.args.get('interval', default='5 minutes')
    hours = request.args.get('hours', default=6, type=int)
    
    df = store.get_log_volume_histogram(interval=interval, limit_hours=hours)
    return jsonify(df.to_dict(orient="records"))