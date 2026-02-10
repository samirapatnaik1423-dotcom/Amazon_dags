"""
Flask Web Dashboard for Airflow API Testing
Modern GUI with enhanced visualizations and data tables
"""
from flask import Flask, render_template, request, jsonify, send_from_directory
import requests
from typing import Dict, Any
import os

app = Flask(__name__)

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "dev-key-12345")
HEADERS = {"X-API-Key": API_KEY}


def make_api_request(endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
    """Make request to API and return response"""
    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        return {
            "success": True,
            "status_code": response.status_code,
            "data": response.json()
        }
    except requests.exceptions.RequestException as e:
        return {
            "success": False,
            "error": str(e)
        }


@app.route("/")
def index():
    """Main modern dashboard page"""
    return render_template("dashboard_modern.html")


@app.route("/classic")
def classic():
    """Classic dashboard page (legacy)"""
    return render_template("dashboard.html")


@app.route("/static/<path:filename>")
def serve_static(filename):
    """Serve static JavaScript and CSS files"""
    return send_from_directory("static", filename)


@app.route("/api/health")
def health_check():
    """Check API health"""
    return jsonify(make_api_request("/health"))


@app.route("/api/dags")
def get_dags():
    """Get all DAGs"""
    return jsonify(make_api_request("/api/v1/dags"))


@app.route("/api/dags/<dag_id>/status")
def get_dag_status(dag_id: str):
    """Get DAG status"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/status"))


@app.route("/api/dags/<dag_id>/runs")
def get_dag_runs(dag_id: str):
    """Get DAG runs with optional filters"""
    params = {
        "page": request.args.get("page", 1),
        "page_size": request.args.get("page_size", 50),
    }
    if request.args.get("state"):
        params["state"] = request.args.get("state")
    
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs", params))


@app.route("/api/dags/<dag_id>/runs/<run_id>")
def get_dag_run(dag_id: str, run_id: str):
    """Get specific DAG run"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs/{run_id}"))


@app.route("/api/dags/<dag_id>/runs/<run_id>/tasks")
def get_run_tasks(dag_id: str, run_id: str):
    """Get tasks for a run"""
    return jsonify(make_api_request(f"/api/v1/dags/{dag_id}/runs/{run_id}/tasks"))


@app.route("/api/metadata/summary")
def get_metadata_summary():
    """Get metadata summary"""
    return jsonify(make_api_request("/api/v1/metadata/summary"))


@app.route("/api/metadata/tables")
def get_table_stats():
    """Get table statistics"""
    return jsonify(make_api_request("/api/v1/metadata/tables"))


@app.route("/api/metadata/metrics")
def get_metrics():
    """Get pipeline metrics"""
    return jsonify(make_api_request("/api/v1/metadata/metrics"))


@app.route("/api/logs/<dag_id>")
def get_dag_logs(dag_id: str):
    """List available logs for DAG"""
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}"))


@app.route("/api/logs/<dag_id>/<run_id>")
def get_run_logs(dag_id: str, run_id: str):
    """Get logs for DAG run"""
    params = {}
    if request.args.get("max_lines"):
        params["max_lines"] = request.args.get("max_lines")
    
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}/runs/{run_id}", params))


@app.route("/api/logs/<dag_id>/<run_id>/<task_id>")
def get_task_logs(dag_id: str, run_id: str, task_id: str):
    """Get logs for task"""
    params = {
        "try_number": request.args.get("try_number", 1)
    }
    if request.args.get("max_lines"):
        params["max_lines"] = request.args.get("max_lines")
    
    return jsonify(make_api_request(f"/api/v1/logs/dags/{dag_id}/runs/{run_id}/tasks/{task_id}", params))


# ═══════════════════════════════════════════════════════════════════════
# PHASE 1 - Data Ingestion Endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/ingestion/logs")
def get_ingestion_logs():
    """Get ingestion logs"""
    params = {}
    if request.args.get("status"):
        params["status"] = request.args.get("status")
    if request.args.get("limit"):
        params["limit"] = request.args.get("limit")
    return jsonify(make_api_request("/ingestion/logs", params))


@app.route("/api/ingestion/connections")
def get_api_connections():
    """Get API connections"""
    return jsonify(make_api_request("/ingestion/connections"))


@app.route("/api/ingestion/file-history")
def get_file_history():
    """Get file watch history"""
    params = {}
    if request.args.get("limit"):
        params["limit"] = request.args.get("limit")
    return jsonify(make_api_request("/ingestion/file-history", params))


# ═══════════════════════════════════════════════════════════════════════
# PHASE 1 - Quality & Validation Endpoints (T0012)
# ═══════════════════════════════════════════════════════════════════════

@app.route("/api/quality/summary")
def get_quality_summary():
    """Get comprehensive quality summary"""
    return jsonify(make_api_request("/quality/summary"))


@app.route("/api/quality/scorecard")
def get_quality_scorecard():
    """Get quality scorecards for all tables"""
    return jsonify(make_api_request("/quality/scorecard"))


@app.route("/api/quality/scorecard/<table_name>")
def get_table_scorecard(table_name: str):
    """Get quality scorecard for specific table"""
    return jsonify(make_api_request(f"/quality/scorecard/{table_name}"))


@app.route("/api/downloads/list")
def list_downloads():
    """List all available downloadable files"""
    import os
    
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    
    categories = {
        'bronze': {'path': os.path.join(base_path, 'bronze'), 'label': 'Bronze Layer (Raw Data)'},
        'silver': {'path': os.path.join(base_path, 'silver'), 'label': 'Silver Layer (Cleaned Data)'},
        'gold': {'path': os.path.join(base_path, 'gold'), 'label': 'Gold Layer (Reports & Analytics)'},
        'processed': {'path': os.path.join(base_path, 'processed'), 'label': 'Processed Data'},
        'reports': {'path': os.path.join(base_path, 'reports'), 'label': 'Generated Reports'}
    }
    
    result = {}
    for category, info in categories.items():
        files = []
        if os.path.exists(info['path']):
            for filename in os.listdir(info['path']):
                filepath = os.path.join(info['path'], filename)
                if os.path.isfile(filepath) and filename.endswith('.csv'):
                    size = os.path.getsize(filepath)
                    files.append({
                        'name': filename,
                        'size': size,
                        'size_mb': round(size / (1024 * 1024), 2),
                        'modified': os.path.getmtime(filepath)
                    })
        result[category] = {
            'label': info['label'],
            'files': sorted(files, key=lambda x: x['name'])
        }
    
    return jsonify({'success': True, 'data': result})


@app.route("/api/downloads/preview/<category>/<filename>")
def preview_file(category: str, filename: str):
    """Preview first few rows of a CSV file"""
    import pandas as pd
    import os
    
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    file_path = os.path.join(base_path, category, filename)
    
    if not os.path.exists(file_path) or not filename.endswith('.csv'):
        return jsonify({'success': False, 'error': 'File not found'})
    
    try:
        # Try different encodings to handle various file formats
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, nrows=50, encoding=encoding)
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        if df is None:
            return jsonify({'success': False, 'error': 'Could not decode file with any supported encoding'})
        
        preview_data = {
            'columns': df.columns.tolist(),
            'rows': df.to_dict('records'),
            'total_rows': sum(1 for _ in open(file_path, encoding='utf-8', errors='ignore')) - 1,
            'total_columns': len(df.columns)
        }
        return jsonify({'success': True, 'data': preview_data})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route("/api/downloads/file/<category>/<filename>")
def download_file(category: str, filename: str):
    """Download a file"""
    from flask import send_file
    import os
    
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    directory = os.path.join(base_path, category)
    
    if not os.path.exists(os.path.join(directory, filename)):
        return jsonify({'success': False, 'error': 'File not found'}), 404
    
    return send_file(os.path.join(directory, filename), as_attachment=True)


@app.route("/api/quality/validation-results")
def get_validation_results():
    """Get column-level validation results"""
    params = {}
    if request.args.get("table"):
        params["table"] = request.args.get("table")
    if request.args.get("failed_only"):
        params["failed_only"] = request.args.get("failed_only")
    return jsonify(make_api_request("/quality/validation-results", params))


@app.route("/api/quality/anomalies")
def get_anomalies():
    """Get detected anomalies"""
    params = {}
    if request.args.get("table"):
        params["table"] = request.args.get("table")
    if request.args.get("column"):
        params["column"] = request.args.get("column")
    if request.args.get("limit"):
        params["limit"] = request.args.get("limit")
    return jsonify(make_api_request("/quality/anomalies", params))


@app.route("/api/quality/profiles")
def get_data_profiles():
    """Get data profiling information"""
    params = {}
    if request.args.get("table"):
        params["table"] = request.args.get("table")
    return jsonify(make_api_request("/quality/profiles", params))


@app.route("/api/quality/quality-checks")
def get_quality_checks():
    """Get quality rule check results"""
    params = {}
    if request.args.get("table"):
        params["table"] = request.args.get("table")
    if request.args.get("failed_only"):
        params["failed_only"] = request.args.get("failed_only")
    return jsonify(make_api_request("/quality/quality-checks", params))


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 Airflow API Web Dashboard")
    print("=" * 60)
    print(f"API Endpoint: {API_BASE_URL}")
    print(f"API Key: {API_KEY[:15]}...")
    print(f"\n📱 Dashboard URL: http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)
