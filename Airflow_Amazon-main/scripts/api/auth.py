# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Authentication
# Task: T0033
# ═══════════════════════════════════════════════════════════════════════

"""
auth.py - API Authentication & Security

TASK IMPLEMENTED:
- T0033: Build Flask/FastAPI service (Authentication layer)

Provides API key-based authentication for securing endpoints.
"""

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
from typing import Optional

from .config import config


# ========================================
# Team 1 - T0033: API Key Authentication
# ========================================

api_key_header = APIKeyHeader(name=config.API_KEY_HEADER, auto_error=False)


async def get_api_key(api_key: Optional[str] = Security(api_key_header)) -> str:
    """
    Validate API key from request header.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        Valid API key string
        
    Raises:
        HTTPException: If API key is invalid or missing
    """
    # If authentication is disabled, allow all requests
    if not config.API_KEY_ENABLED:
        return "auth-disabled"
    
    # Check if API key is provided
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key is required. Provide it in the X-API-Key header.",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    # Validate API key
    if api_key not in config.VALID_API_KEYS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API key. Access denied.",
        )
    
    return api_key


# ========================================
# Team 1 - T0033: Optional Authentication Decorator
# ========================================

async def get_optional_api_key(api_key: Optional[str] = Security(api_key_header)) -> Optional[str]:
    """
    Optional API key validation for public endpoints.
    
    Args:
        api_key: API key from X-API-Key header
        
    Returns:
        Valid API key string or None
    """
    if not api_key or not config.API_KEY_ENABLED:
        return None
    
    if api_key in config.VALID_API_KEYS:
        return api_key
    
    return None
