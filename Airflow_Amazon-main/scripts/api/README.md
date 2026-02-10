# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: REST API SERVICE
# Tasks: T0033-T0037
# ═══════════════════════════════════════════════════════════════════════

# Airflow ETL Pipeline REST API

A comprehensive REST API service for monitoring and managing Airflow ETL pipelines.

## 📋 Overview

This API provides endpoints for:
- **DAG Status Monitoring** (T0034) - Real-time pipeline status and execution history
- **Metadata Summary** (T0035) - Pipeline statistics and table information
- **Log Retrieval** (T0036) - Access to execution logs
- **Pagination & Filtering** (T0037) - Efficient data querying

## 🚀 Quick Start

### Prerequisites
```bash
pip install fastapi uvicorn python-multipart httpx
```

### Start API Server

**Development Mode:**
```bash
cd d:\sam\Projects\Infosys\Airflow
uvicorn scripts.api.main:app --reload --host 0.0.0.0 --port 8000
```

**Production Mode:**
```bash
uvicorn scripts.api.main:app --workers 4 --host 0.0.0.0 --port 8000
```

### Access API Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## 🔐 Authentication

The API uses **API Key** authentication via the `X-API-Key` header.

### Default API Keys (Development):
- `dev-key-12345`
- `test-key-67890`

### Example Request:
```bash
curl -H "X-API-Key: dev-key-12345" http://localhost:8000/api/v1/dags
```

### Environment Variables:
```env
API_KEY_ENABLED=true
API_KEYS=dev-key-12345,test-key-67890
```

## 📡 API Endpoints

### Health Check
```
GET /health
GET /
```
Check API health and database connectivity.

### DAGs (T0034)
```
GET  /api/v1/dags                           # List all DAGs
GET  /api/v1/dags/{dag_id}/status           # Get DAG status
GET  /api/v1/dags/{dag_id}/runs             # Get DAG runs (paginated)
GET  /api/v1/dags/{dag_id}/runs/{run_id}    # Get specific run details
GET  /api/v1/dags/{dag_id}/runs/{run_id}/tasks  # Get run tasks
```

### Metadata (T0035)
```
GET  /api/v1/metadata/summary               # Get complete metadata summary
GET  /api/v1/metadata/tables                # Get table statistics
GET  /api/v1/metadata/metrics               # Get pipeline metrics
```

### Logs (T0036)
```
GET  /api/v1/logs/dags/{dag_id}                     # List available logs
GET  /api/v1/logs/dags/{dag_id}/runs/{run_id}       # Get DAG run logs
GET  /api/v1/logs/dags/{dag_id}/runs/{run_id}/tasks/{task_id}  # Get task logs
```

## 📝 Example Requests

### 1. List All DAGs
```bash
curl -H "X-API-Key: dev-key-12345" \
  http://localhost:8000/api/v1/dags
```

**Response:**
```json
[
  {
    "dag_id": "etl_customers",
    "description": "Customers Table ETL Pipeline",
    "schedule_interval": "30 18 * * *",
    "is_paused": false,
    "is_active": true,
    "tags": ["team1", "etl"],
    "owners": ["team1"]
  }
]
```

### 2. Get DAG Status
```bash
curl -H "X-API-Key: dev-key-12345" \
  http://localhost:8000/api/v1/dags/etl_customers/status
```

**Response:**
```json
{
  "dag_info": {
    "dag_id": "etl_customers",
    "is_paused": false,
    "is_active": true
  },
  "latest_run": {
    "run_id": "manual__2026-01-19T10:00:00+00:00",
    "state": "success",
    "duration_seconds": 145.5
  },
  "total_runs": 45,
  "success_count": 42,
  "failed_count": 3
}
```

### 3. Get DAG Runs (Paginated)
```bash
curl -H "X-API-Key: dev-key-12345" \
  "http://localhost:8000/api/v1/dags/etl_customers/runs?page=1&page_size=10&state=success"
```

**Response:**
```json
{
  "dag_id": "etl_customers",
  "runs": [...],
  "total": 42,
  "page": 1,
  "page_size": 10,
  "has_next": true
}
```

### 4. Get Metadata Summary
```bash
curl -H "X-API-Key: dev-key-12345" \
  http://localhost:8000/api/v1/metadata/summary
```

**Response:**
```json
{
  "pipeline_name": "Airflow ETL Pipeline API",
  "total_dags": 7,
  "active_dags": 7,
  "paused_dags": 0,
  "total_runs_today": 14,
  "successful_runs_today": 14,
  "failed_runs_today": 0,
  "tables": [
    {
      "table_name": "customers",
      "schema_name": "etl_output",
      "row_count": 15266
    }
  ]
}
```

### 5. Get Task Logs
```bash
curl -H "X-API-Key: dev-key-12345" \
  "http://localhost:8000/api/v1/logs/dags/etl_customers/runs/manual__2026-01-19T10:00:00+00:00/tasks/extract?try_number=1"
```

**Response:**
```json
{
  "dag_id": "etl_customers",
  "run_id": "manual__2026-01-19T10:00:00+00:00",
  "task_id": "extract",
  "try_number": 1,
  "log_size": 2345,
  "content": "[2026-01-19 10:00:05] INFO - Starting task...",
  "truncated": false
}
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file or set environment variables:

```env
# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=false

# Authentication
API_KEY_ENABLED=true
API_KEYS=your-secure-key-1,your-secure-key-2

# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
ETL_SCHEMA=etl_output

# Pagination
DEFAULT_PAGE_SIZE=50
MAX_PAGE_SIZE=500

# Logs
AIRFLOW_LOGS=/opt/airflow/logs
MAX_LOG_SIZE=100000
```

## 🏗️ Project Structure

```
scripts/api/
├── __init__.py              # Package init
├── main.py                  # FastAPI application (T0033)
├── config.py                # Configuration settings (T0033)
├── auth.py                  # API key authentication (T0033)
├── models/
│   ├── __init__.py
│   ├── dag_models.py        # DAG data models (T0034)
│   └── response_models.py   # API response models (T0035, T0037)
├── routes/
│   ├── __init__.py
│   ├── health.py            # Health check endpoint (T0033)
│   ├── dags.py              # DAG endpoints (T0034)
│   ├── metadata.py          # Metadata endpoints (T0035)
│   └── logs.py              # Log endpoints (T0036)
└── utils/
    ├── __init__.py
    ├── pagination.py        # Pagination utilities (T0037)
    ├── filters.py           # Query filters (T0037)
    └── airflow_client.py    # Database client (T0034)
```

## 🧪 Testing

### Manual Testing with curl:
```bash
# Health check
curl http://localhost:8000/health

# List DAGs (with authentication)
curl -H "X-API-Key: dev-key-12345" \
  http://localhost:8000/api/v1/dags

# Get DAG status
curl -H "X-API-Key: dev-key-12345" \
  http://localhost:8000/api/v1/dags/etl_customers/status
```

### Testing with Python:
```python
import httpx

client = httpx.Client(
    base_url="http://localhost:8000",
    headers={"X-API-Key": "dev-key-12345"}
)

# Get all DAGs
response = client.get("/api/v1/dags")
dags = response.json()
print(f"Total DAGs: {len(dags)}")

# Get metadata
response = client.get("/api/v1/metadata/summary")
metadata = response.json()
print(f"Active DAGs: {metadata['active_dags']}")
```

## 🔒 Security Considerations

### Production Deployment:
1. **Change Default API Keys**:
   ```env
   API_KEYS=your-secure-random-key-here
   ```

2. **Enable HTTPS**: Use a reverse proxy (nginx/traefik) with SSL/TLS

3. **Rate Limiting**: Add rate limiting middleware

4. **Database Credentials**: Use secrets management (Vault, AWS Secrets Manager)

5. **Restrict CORS Origins**:
   ```env
   CORS_ORIGINS=https://your-frontend.com
   ```

## 📊 Task Implementation

| Task | Description | Status | Files |
|------|-------------|--------|-------|
| T0033 | Build FastAPI service | ✅ | main.py, config.py, auth.py, health.py |
| T0034 | Expose pipeline run status | ✅ | dags.py, dag_models.py, airflow_client.py |
| T0035 | Expose metadata summary | ✅ | metadata.py, response_models.py |
| T0036 | Fetch logs via API | ✅ | logs.py |
| T0037 | Pagination & filtering | ✅ | pagination.py, filters.py |

## 🚀 Integration with Docker

To run the API inside Docker with Airflow:

1. Update `docker-compose.yaml`:
```yaml
  api-server:
    image: apache/airflow:2.8.3
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    volumes:
      - ../scripts:/opt/airflow/scripts
      - ../logs:/opt/airflow/logs
    ports:
      - "8000:8000"
    command: uvicorn scripts.api.main:app --host 0.0.0.0 --port 8000
```

2. Rebuild and start:
```bash
docker-compose up -d api-server
```

## 📚 Additional Resources

- **FastAPI Documentation**: https://fastapi.tiangolo.com
- **Swagger UI**: http://localhost:8000/docs
- **Project Tracker**: See PROJECT_TRACKER.md for Sprint 7 details

---

**Version**: 1.0.0  
**Last Updated**: January 19, 2026  
**Team**: Team 1  
**Sprint**: Sprint 7 (T0033-T0037)
