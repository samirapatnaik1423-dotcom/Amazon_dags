# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Data Models Package
# Tasks: T0034, T0035
# ═══════════════════════════════════════════════════════════════════════

"""
API Models Package - Pydantic schemas for API responses

Provides data models for:
- DAG status and runs (T0034)
- Metadata summary (T0035)
"""

from .dag_models import (
    DAGInfo,
    DAGRunInfo,
    TaskInstanceInfo,
    DAGStatusResponse,
    DAGRunsResponse,
)
from .response_models import (
    HealthResponse,
    MetadataSummary,
    TableStats,
    PaginatedResponse,
    ErrorResponse,
)

__all__ = [
    # DAG Models
    "DAGInfo",
    "DAGRunInfo",
    "TaskInstanceInfo",
    "DAGStatusResponse",
    "DAGRunsResponse",
    # Response Models
    "HealthResponse",
    "MetadataSummary",
    "TableStats",
    "PaginatedResponse",
    "ErrorResponse",
]
