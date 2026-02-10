# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Response Models
# Tasks: T0035, T0037
# ═══════════════════════════════════════════════════════════════════════

"""
response_models.py - Pydantic models for API responses

TASKS IMPLEMENTED:
- T0035: Expose metadata summary (Data models)
- T0037: Pagination & filtering (Pagination response model)

Provides schema definitions for:
- Health check response
- Metadata summary
- Table statistics
- Paginated responses
- Error responses
"""

from datetime import datetime
from typing import Optional, List, Dict, Any, Generic, TypeVar
from pydantic import BaseModel, Field


# ========================================
# Team 1 - T0033: Health Check Response
# ========================================

class HealthResponse(BaseModel):
    """API health check response"""
    status: str = Field(..., description="Health status (healthy, unhealthy)")
    version: str = Field(..., description="API version")
    timestamp: datetime = Field(..., description="Current server timestamp")
    database_connected: bool = Field(..., description="Database connection status")
    
    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "timestamp": "2026-01-19T10:00:00",
                "database_connected": True
            }
        }


# ========================================
# Team 1 - T0035: Table Statistics Model
# ========================================

class TableStats(BaseModel):
    """Statistics for a single table"""
    table_name: str = Field(..., description="Table name")
    schema_name: str = Field(..., description="Database schema")
    row_count: int = Field(..., description="Total number of rows")
    last_updated: Optional[datetime] = Field(None, description="Last update timestamp")
    size_bytes: Optional[int] = Field(None, description="Table size in bytes")
    columns: Optional[List[str]] = Field(None, description="Column names")
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_name": "customers",
                "schema_name": "etl_output",
                "row_count": 15266,
                "last_updated": "2026-01-19T10:00:00",
                "size_bytes": 2048576,
                "columns": ["CustomerKey", "Name", "Email", "Birthday", "Age"]
            }
        }


# ========================================
# Team 1 - T0035: Metadata Summary Response Model
# ========================================

class MetadataSummary(BaseModel):
    """Complete pipeline metadata summary"""
    pipeline_name: str = Field(..., description="Pipeline name")
    total_dags: int = Field(..., description="Total number of DAGs")
    active_dags: int = Field(..., description="Number of active DAGs")
    paused_dags: int = Field(..., description="Number of paused DAGs")
    total_runs_today: int = Field(..., description="Total runs executed today")
    successful_runs_today: int = Field(..., description="Successful runs today")
    failed_runs_today: int = Field(..., description="Failed runs today")
    tables: List[TableStats] = Field(..., description="Table statistics")
    last_refresh: datetime = Field(..., description="Metadata refresh timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "pipeline_name": "Airflow ETL Pipeline",
                "total_dags": 7,
                "active_dags": 7,
                "paused_dags": 0,
                "total_runs_today": 14,
                "successful_runs_today": 14,
                "failed_runs_today": 0,
                "tables": [],
                "last_refresh": "2026-01-19T10:00:00"
            }
        }


# ========================================
# Team 1 - T0037: Generic Paginated Response Model
# ========================================

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper"""
    items: List[T] = Field(..., description="List of items")
    total: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number (1-indexed)")
    page_size: int = Field(..., description="Items per page")
    total_pages: int = Field(..., description="Total number of pages")
    has_next: bool = Field(..., description="Whether next page exists")
    has_previous: bool = Field(..., description="Whether previous page exists")
    
    class Config:
        json_schema_extra = {
            "example": {
                "items": [],
                "total": 100,
                "page": 1,
                "page_size": 50,
                "total_pages": 2,
                "has_next": True,
                "has_previous": False
            }
        }


# ========================================
# Team 1 - T0033: Error Response Model
# ========================================

class ErrorResponse(BaseModel):
    """Standard error response"""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(..., description="Error timestamp")
    path: Optional[str] = Field(None, description="Request path")
    
    class Config:
        json_schema_extra = {
            "example": {
                "error": "NotFound",
                "message": "DAG not found",
                "detail": "DAG with id 'etl_unknown' does not exist",
                "timestamp": "2026-01-19T10:00:00",
                "path": "/api/v1/dags/etl_unknown/status"
            }
        }
