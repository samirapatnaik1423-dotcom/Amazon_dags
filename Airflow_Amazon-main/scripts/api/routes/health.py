# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Health Check Endpoint
# Task: T0033
# ═══════════════════════════════════════════════════════════════════════

"""
health.py - Health check endpoint

TASK IMPLEMENTED:
- T0033: Build Flask/FastAPI service (Health check endpoint)

Provides endpoint for checking API health and database connectivity.
"""

from datetime import datetime
from fastapi import APIRouter, status

from ..models.response_models import HealthResponse
from ..config import config
from ..utils.airflow_client import AirflowClient


# ========================================
# Team 1 - T0033: Health Check Router
# ========================================

router = APIRouter(
    prefix="",
    tags=["Health"],
    responses={
        200: {"description": "API is healthy"},
        503: {"description": "API is unhealthy"}
    }
)


# ========================================
# Team 1 - T0033: Health Check Endpoint
# ========================================

@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Check API health and database connectivity"
)
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        HealthResponse with API status and database connectivity
    """
    # Test database connection
    client = AirflowClient()
    db_connected = client.test_connection()
    
    # Determine overall status
    overall_status = "healthy" if db_connected else "unhealthy"
    
    # Return status code based on health
    response = HealthResponse(
        status=overall_status,
        version=config.VERSION,
        timestamp=datetime.now(),
        database_connected=db_connected
    )
    
    return response


@router.get(
    "/",
    summary="API Root",
    description="Get API information"
)
async def root():
    """
    Root endpoint - API information.
    
    Returns:
        Basic API information
    """
    return {
        "name": config.APP_NAME,
        "version": config.VERSION,
        "description": config.DESCRIPTION,
        "docs_url": "/docs",
        "health_url": "/health",
        "api_url": "/api/v1"
    }
