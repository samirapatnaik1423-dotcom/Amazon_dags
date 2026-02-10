# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: API Configuration
# Task: T0033
# ═══════════════════════════════════════════════════════════════════════

"""
config.py - API Configuration Settings

TASK IMPLEMENTED:
- T0033: Build Flask/FastAPI service (Configuration layer)

Provides centralized configuration for the API service including:
- Database connection settings
- Authentication settings
- CORS configuration
- API metadata
"""

import os
from typing import List
from pydantic import BaseModel


def _default_db_host_port():
    """Return default DB host/port based on runtime environment."""
    if os.getenv("POSTGRES_HOST") or os.getenv("POSTGRES_PORT"):
        return os.getenv("POSTGRES_HOST", "localhost"), os.getenv("POSTGRES_PORT", "5434")
    # If running inside Docker, prefer service DNS name
    if os.path.exists("/.dockerenv"):
        return "postgres", "5432"
    # Local development defaults
    return "localhost", "5434"


# ========================================
# Team 1 - T0033: API Service Configuration
# ========================================

class APIConfig(BaseModel):
    """API Configuration Settings"""
    
    # API Metadata
    APP_NAME: str = "Airflow ETL Pipeline API"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "REST API for monitoring and managing ETL pipelines"
    
    # Server Configuration
    HOST: str = os.getenv("API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("API_PORT", "8000"))
    DEBUG: bool = os.getenv("API_DEBUG", "false").lower() == "true"
    
    # ========================================
    # Team 1 - T0033: Database Connection Settings
    # ========================================
    # PostgreSQL Connection (Airflow Metadata DB)
    _db_host, _db_port = _default_db_host_port()
    DB_HOST: str = _db_host
    DB_PORT: int = int(_db_port)
    DB_NAME: str = os.getenv("POSTGRES_DB", "airflow")
    DB_USER: str = os.getenv("POSTGRES_USER", "airflow")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "airflow")
    DB_SCHEMA: str = os.getenv("ETL_SCHEMA", "etl_output")
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    # ========================================
    # Team 1 - T0033: Authentication Settings
    # ========================================
    # API Key Authentication
    API_KEY_ENABLED: bool = os.getenv("API_KEY_ENABLED", "true").lower() == "true"
    API_KEY_HEADER: str = "X-API-Key"
    VALID_API_KEYS: List[str] = os.getenv("API_KEYS", "dev-key-12345,test-key-67890").split(",")
    
    # ========================================
    # Team 1 - T0033: CORS Configuration
    # ========================================
    CORS_ENABLED: bool = True
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080", "*"]
    CORS_METHODS: List[str] = ["GET", "POST", "PUT", "DELETE"]
    CORS_HEADERS: List[str] = ["*"]
    
    # ========================================
    # Team 1 - T0037: Pagination Defaults
    # ========================================
    DEFAULT_PAGE_SIZE: int = 50
    MAX_PAGE_SIZE: int = 500
    
    # ========================================
    # Team 1 - T0036: Log Settings
    # ========================================
    LOGS_DIR: str = os.getenv("AIRFLOW_LOGS", "/opt/airflow/logs")
    MAX_LOG_SIZE: int = 100000  # Max characters to return
    
    class Config:
        case_sensitive = True


# Global configuration instance
config = APIConfig()
