# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: FastAPI Application
# Task: T0033
# ═══════════════════════════════════════════════════════════════════════

"""
main.py - FastAPI Application Entry Point

TASK IMPLEMENTED:
- T0033: Build Flask/FastAPI service (Main application)

Complete REST API service for monitoring Airflow ETL pipelines.

Features:
- DAG status and run history (T0034)
- Metadata summary (T0035)
- Log retrieval (T0036)
- Pagination and filtering (T0037)
- API key authentication
- Auto-generated Swagger documentation
- CORS support

Usage:
    # Development
    uvicorn scripts.api.main:app --reload --host 0.0.0.0 --port 8000
    
    # Production
    uvicorn scripts.api.main:app --workers 4 --host 0.0.0.0 --port 8000

API Documentation:
    http://localhost:8000/docs (Swagger UI)
    http://localhost:8000/redoc (ReDoc)
"""

from datetime import datetime
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from .config import config
from .routes import health_router, dags_router, metadata_router, logs_router
from .routes_ingestion import router as ingestion_router  # PHASE 1
from .routes_quality import router as quality_router  # PHASE 1 - T0012


# ========================================
# Team 1 - T0033: FastAPI Application Instance
# ========================================

app = FastAPI(
    title=config.APP_NAME,
    description=config.DESCRIPTION,
    version=config.VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={
        "syntaxHighlight.theme": "monokai",
        "tryItOutEnabled": True
    }
)

# ========================================
# Phase 3 - Monitoring & Observability
# ========================================

REQUEST_COUNT = Counter(
    "api_requests_total",
    "Total API requests",
    ["method", "path", "status"]
)

REQUEST_LATENCY = Histogram(
    "api_request_latency_seconds",
    "API request latency in seconds",
    ["method", "path"]
)


# ========================================
# Team 1 - T0033: CORS Middleware Configuration
# ========================================

if config.CORS_ENABLED:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=config.CORS_METHODS,
        allow_headers=config.CORS_HEADERS,
    )


# ========================================
# Team 1 - T0033: Global Exception Handlers
# ========================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors"""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "ValidationError",
            "message": "Invalid request parameters",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat(),
            "path": str(request.url)
        }
    )


@app.exception_handler(SQLAlchemyError)
async def database_exception_handler(request: Request, exc: SQLAlchemyError):
    """Handle database errors"""
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": "DatabaseError",
            "message": "Database operation failed",
            "detail": "Unable to connect to or query the database",
            "timestamp": datetime.now().isoformat(),
            "path": str(request.url)
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected errors"""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "detail": str(exc) if config.DEBUG else "Internal server error",
            "timestamp": datetime.now().isoformat(),
            "path": str(request.url)
        }
    )


# ========================================
# Team 1 - T0033: Request Logging Middleware
# ========================================

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests"""
    start_time = datetime.now()
    
    # Process request
    response = await call_next(request)
    
    # Calculate duration
    duration = (datetime.now() - start_time).total_seconds()
    
    # Log request details
    print(f"{request.method} {request.url.path} - {response.status_code} - {duration:.3f}s")

    # Prometheus metrics
    REQUEST_COUNT.labels(request.method, request.url.path, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(request.method, request.url.path).observe(duration)
    
    # Add custom headers
    response.headers["X-Process-Time"] = f"{duration:.3f}s"
    response.headers["X-API-Version"] = config.VERSION
    
    return response


# ========================================
# Phase 3 - Prometheus Metrics Endpoint
# ========================================

@app.get("/metrics", include_in_schema=False)
def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ========================================
# Team 1 - T0033: Register API Routes
# ========================================

# Health check (no prefix, public)
app.include_router(health_router)

# DAG endpoints (T0034)
app.include_router(dags_router)

# Metadata endpoints (T0035)
app.include_router(metadata_router)

# Log endpoints (T0036)
app.include_router(logs_router)

# Phase 1 ingestion endpoints (TEAM 2)
app.include_router(ingestion_router)

# Phase 1 quality & validation endpoints (TEAM 2 - T0012)
app.include_router(quality_router)


# ========================================
# Team 1 - T0033: Startup and Shutdown Events
# ========================================

@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    print("=" * 60)
    print(f"🚀 {config.APP_NAME} v{config.VERSION}")
    print(f"📡 Starting API server on {config.HOST}:{config.PORT}")
    print(f"📚 API Documentation: http://{config.HOST}:{config.PORT}/docs")
    print(f"🔐 Authentication: {'Enabled' if config.API_KEY_ENABLED else 'Disabled'}")
    print(f"🗄️  Database: {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    print("=" * 60)


@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    print("\n" + "=" * 60)
    print("🛑 Shutting down API server")
    print("=" * 60)


# ========================================
# Team 1 - T0033: Main Entry Point
# ========================================

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.DEBUG,
        log_level="info"
    )
