# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Query Filters Utility
# Task: T0037
# ═══════════════════════════════════════════════════════════════════════

"""
filters.py - Query filtering utilities

TASK IMPLEMENTED:
- T0037: Pagination & filtering

Provides utilities for:
- Date range filtering
- Status filtering
- DAG ID filtering
- Custom filter building
"""

from datetime import datetime, timedelta
from typing import Optional, List, Any, Tuple
from sqlalchemy import Column, and_, or_, func
from sqlalchemy.sql.expression import BinaryExpression


# ========================================
# Team 1 - T0037: Filter Builder Class
# ========================================

class FilterBuilder:
    """Helper class for building SQL filters"""
    
    def __init__(self):
        """Initialize empty filter list"""
        self.filters: List[BinaryExpression] = []
    
    def add_filter(self, condition: BinaryExpression) -> 'FilterBuilder':
        """
        Add a filter condition.
        
        Args:
            condition: SQLAlchemy filter expression
            
        Returns:
            Self for method chaining
        """
        if condition is not None:
            self.filters.append(condition)
        return self
    
    def add_equals(self, column: Column, value: Any) -> 'FilterBuilder':
        """
        Add equality filter.
        
        Args:
            column: SQLAlchemy column
            value: Value to match
            
        Returns:
            Self for method chaining
        """
        if value is not None:
            self.filters.append(column == value)
        return self
    
    def add_in(self, column: Column, values: Optional[List[Any]]) -> 'FilterBuilder':
        """
        Add IN filter.
        
        Args:
            column: SQLAlchemy column
            values: List of values
            
        Returns:
            Self for method chaining
        """
        if values:
            self.filters.append(column.in_(values))
        return self
    
    def add_date_range(
        self,
        column: Column,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> 'FilterBuilder':
        """
        Add date range filter.
        
        Args:
            column: Date/datetime column
            start_date: Start of range (inclusive)
            end_date: End of range (inclusive)
            
        Returns:
            Self for method chaining
        """
        if start_date:
            self.filters.append(column >= start_date)
        if end_date:
            self.filters.append(column <= end_date)
        return self
    
    def build(self) -> Optional[Any]:
        """
        Build combined filter expression.
        
        Returns:
            SQLAlchemy AND expression or None
        """
        if not self.filters:
            return None
        if len(self.filters) == 1:
            return self.filters[0]
        return and_(*self.filters)


# ========================================
# Team 1 - T0037: Filter Helper Functions
# ========================================

def parse_date_filter(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse date string to datetime.
    
    Args:
        date_str: Date string in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)
        
    Returns:
        Datetime object or None
        
    Example:
        >>> parse_date_filter("2026-01-19")
        datetime(2026, 1, 19, 0, 0)
    """
    if not date_str:
        return None
    
    try:
        # Try parsing with time
        return datetime.fromisoformat(date_str)
    except ValueError:
        try:
            # Try parsing date only
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None


def get_today_range() -> Tuple[datetime, datetime]:
    """
    Get start and end of today.
    
    Returns:
        Tuple of (start_of_day, end_of_day)
    """
    now = datetime.now()
    start_of_day = datetime(now.year, now.month, now.day, 0, 0, 0)
    end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59)
    return start_of_day, end_of_day


def get_last_n_days_range(days: int) -> tuple[datetime, datetime]:
    """
    Get date range for last N days.
    
    Args:
        days: Number of days to look back
        
    Returns:
        Tuple of (start_date, end_date)
    """
    now = datetime.now()
    end_date = now
    start_date = now - timedelta(days=days)
    return start_date, end_date


def parse_state_filter(state: Optional[str]) -> Optional[str]:
    """
    Parse and validate state filter.
    
    Args:
        state: State string (success, failed, running, etc.)
        
    Returns:
        Validated state string or None
    """
    if not state:
        return None
    
    valid_states = [
        'success', 'failed', 'running', 'queued',
        'scheduled', 'upstream_failed', 'skipped',
        'up_for_retry', 'up_for_reschedule', 'removed'
    ]
    
    state_lower = state.lower()
    if state_lower in valid_states:
        return state_lower
    
    return None
