# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Pagination Utility
# Task: T0037
# ═══════════════════════════════════════════════════════════════════════

"""
pagination.py - Pagination helper functions

TASK IMPLEMENTED:
- T0037: Pagination & filtering

Provides utilities for:
- Page calculation
- Offset calculation
- Paginated response creation
"""

from typing import List, TypeVar, Generic, Tuple
from math import ceil

from ..models.response_models import PaginatedResponse
from ..config import config


T = TypeVar('T')


# ========================================
# Team 1 - T0037: Pagination Helper Class
# ========================================

class Paginator(Generic[T]):
    """Helper class for pagination logic"""
    
    def __init__(
        self,
        items: List[T],
        page: int = 1,
        page_size: int = None
    ):
        """
        Initialize paginator.
        
        Args:
            items: Full list of items to paginate
            page: Page number (1-indexed)
            page_size: Number of items per page
        """
        self.items = items
        self.page = max(1, page)  # Ensure page >= 1
        self.page_size = page_size or config.DEFAULT_PAGE_SIZE
        
        # Enforce max page size
        if self.page_size > config.MAX_PAGE_SIZE:
            self.page_size = config.MAX_PAGE_SIZE
    
    @property
    def total_items(self) -> int:
        """Total number of items"""
        return len(self.items)
    
    @property
    def total_pages(self) -> int:
        """Total number of pages"""
        if self.total_items == 0:
            return 0
        return ceil(self.total_items / self.page_size)
    
    @property
    def offset(self) -> int:
        """Calculate offset for current page"""
        return (self.page - 1) * self.page_size
    
    @property
    def has_next(self) -> bool:
        """Check if next page exists"""
        return self.page < self.total_pages
    
    @property
    def has_previous(self) -> bool:
        """Check if previous page exists"""
        return self.page > 1
    
    def get_page(self) -> List[T]:
        """Get items for current page"""
        start = self.offset
        end = start + self.page_size
        return self.items[start:end]
    
    def get_response(self) -> PaginatedResponse[T]:
        """
        Create paginated response object.
        
        Returns:
            PaginatedResponse with items and metadata
        """
        return PaginatedResponse(
            items=self.get_page(),
            total=self.total_items,
            page=self.page,
            page_size=self.page_size,
            total_pages=self.total_pages,
            has_next=self.has_next,
            has_previous=self.has_previous
        )


# ========================================
# Team 1 - T0037: Pagination Helper Functions
# ========================================

def paginate(
    items: List[T],
    page: int = 1,
    page_size: int = None
) -> PaginatedResponse[T]:
    """
    Paginate a list of items.
    
    Args:
        items: Full list of items
        page: Page number (1-indexed)
        page_size: Items per page
        
    Returns:
        PaginatedResponse object
        
    Example:
        >>> items = [1, 2, 3, 4, 5]
        >>> response = paginate(items, page=1, page_size=2)
        >>> response.items
        [1, 2]
        >>> response.total
        5
    """
    paginator = Paginator(items, page, page_size)
    return paginator.get_response()


def calculate_offset_limit(page: int, page_size: int = None) -> Tuple[int, int]:
    """
    Calculate SQL OFFSET and LIMIT for pagination.
    
    Args:
        page: Page number (1-indexed)
        page_size: Items per page
        
    Returns:
        Tuple of (offset, limit)
        
    Example:
        >>> calculate_offset_limit(page=2, page_size=10)
        (10, 10)
    """
    page = max(1, page)
    page_size = page_size or config.DEFAULT_PAGE_SIZE
    page_size = min(page_size, config.MAX_PAGE_SIZE)
    
    offset = (page - 1) * page_size
    limit = page_size
    
    return offset, limit
