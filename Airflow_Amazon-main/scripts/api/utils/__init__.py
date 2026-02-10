# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Utilities Package
# Tasks: T0034, T0036, T0037
# ═══════════════════════════════════════════════════════════════════════

"""
API Utilities Package

Provides helper utilities for:
- Airflow database queries (T0034)
- Log file access (T0036)
- Pagination (T0037)
- Filtering (T0037)
"""

from .pagination import paginate, calculate_offset_limit, Paginator
from .filters import FilterBuilder, parse_date_filter, parse_state_filter
from .airflow_client import AirflowClient

__all__ = [
    # Pagination
    "paginate",
    "calculate_offset_limit",
    "Paginator",
    # Filtering
    "FilterBuilder",
    "parse_date_filter",
    "parse_state_filter",
    # Airflow Client
    "AirflowClient",
]
