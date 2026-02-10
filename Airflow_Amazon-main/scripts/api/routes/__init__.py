# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Routes Package
# Tasks: T0033, T0034, T0035, T0036
# ═══════════════════════════════════════════════════════════════════════

"""
API Routes Package

Provides REST API endpoints for:
- Health check (T0033)
- DAG status and runs (T0034)
- Metadata summary (T0035)
- Log retrieval (T0036)
"""

from .health import router as health_router
from .dags import router as dags_router
from .metadata import router as metadata_router
from .logs import router as logs_router

__all__ = [
    "health_router",
    "dags_router",
    "metadata_router",
    "logs_router",
]
