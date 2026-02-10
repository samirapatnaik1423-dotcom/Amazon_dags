"""
Phase 1 Ingestion Routes
TEAM 2 - SPRINT 1 (PHASE 1)
API endpoints for triggering multi-format data ingestion
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from typing import Dict, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
import logging

from .auth import get_api_key

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/ingest",
    tags=["Phase 1 - Multi-Format Ingestion"],
    dependencies=[Depends(get_api_key)]
)


# ═══════════════════════════════════════════════════════════
# REQUEST MODELS
# ═══════════════════════════════════════════════════════════

class JSONIngestRequest(BaseModel):
    file_path: str = Field(..., description="Path to JSON or JSONL file")
    table_name: str = Field(..., description="Target table name")
    normalize: bool = Field(True, description="Flatten nested JSON structures")
    is_jsonl: bool = Field(False, description="True for JSONL format")


class SQLIngestRequest(BaseModel):
    connection_string: str = Field(..., description="SQLAlchemy connection string")
    table_name: str = Field(..., description="Source table name")
    schema: Optional[str] = Field(None, description="Schema name")
    query: Optional[str] = Field(None, description="Custom SQL query (overrides table_name)")
    where_clause: Optional[str] = Field(None, description="WHERE clause filter")


class APIIngestRequest(BaseModel):
    base_url: str = Field(..., description="API base URL")
    endpoint: str = Field(..., description="API endpoint")
    table_name: str = Field(..., description="Target table name")
    auth_type: str = Field("none", description="Authentication type")
    auth_params: Optional[Dict] = Field(None, description="Authentication parameters")
    paginated: bool = Field(False, description="Handle pagination")
    pagination_type: str = Field("offset", description="Pagination type")
    max_pages: Optional[int] = Field(None, description="Maximum pages to fetch")
    pagination_config: Optional[Dict] = Field(None, description="Pagination configuration")
    rate_limit_config: Optional[Dict] = Field(None, description="Rate limiting configuration")


class FileWatcherStatusResponse(BaseModel):
    is_running: bool
    watch_directories: List[str]
    file_patterns: List[str]
    total_processed_files: int
    check_interval_seconds: int


# ═══════════════════════════════════════════════════════════
# JSON INGESTION ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.post("/json", summary="Trigger JSON ingestion")
async def trigger_json_ingestion(request: JSONIngestRequest):
    """
    Trigger JSON/JSONL file ingestion
    
    - **file_path**: Path to JSON file
    - **table_name**: Target table name
    - **normalize**: Flatten nested structures
    - **is_jsonl**: True for JSON Lines format
    """
    logger.info(f"📖 JSON ingestion request: {request.table_name}")
    
    try:
        from scripts.Extract import DataExtractor
        from pathlib import Path
        
        extractor = DataExtractor()
        df, stats = extractor.extract_json(
            file_path=Path(request.file_path),
            table_name=request.table_name,
            normalize=request.normalize,
            is_jsonl=request.is_jsonl
        )
        
        if df is not None:
            # Save to staging
            staging_path = Path(f"data/staging/{request.table_name}_raw.csv")
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(staging_path, index=False)
            
            return {
                "status": "success",
                "table_name": request.table_name,
                "rows_extracted": len(df),
                "columns": len(df.columns),
                "staging_file": str(staging_path),
                "extraction_time_seconds": stats.get('duration_seconds', 0),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="JSON extraction failed")
            
    except Exception as e:
        logger.error(f"❌ JSON ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════
# SQL INGESTION ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.post("/sql", summary="Trigger SQL extraction")
async def trigger_sql_extraction(request: SQLIngestRequest):
    """
    Extract data from SQL database
    
    - **connection_string**: SQLAlchemy connection string
    - **table_name**: Source table name
    - **schema**: Optional schema name
    - **query**: Optional custom SQL query
    - **where_clause**: Optional WHERE clause
    """
    logger.info(f"🗄️ SQL extraction request: {request.table_name}")
    
    try:
        from scripts.Extract import DataExtractor
        from pathlib import Path
        
        extractor = DataExtractor()
        df, stats = extractor.extract_from_sql(
            connection_string=request.connection_string,
            table_name=request.table_name,
            schema=request.schema,
            query=request.query,
            where_clause=request.where_clause
        )
        
        if df is not None:
            # Save to staging
            staging_path = Path(f"data/staging/{request.table_name}_sql_extract.csv")
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(staging_path, index=False)
            
            return {
                "status": "success",
                "table_name": request.table_name,
                "rows_extracted": len(df),
                "columns": len(df.columns),
                "staging_file": str(staging_path),
                "extraction_time_seconds": stats.get('duration_seconds', 0),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="SQL extraction failed")
            
    except Exception as e:
        logger.error(f"❌ SQL extraction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════
# API INGESTION ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.post("/api", summary="Trigger API ingestion")
async def trigger_api_ingestion(request: APIIngestRequest):
    """
    Ingest data from REST API
    
    - **base_url**: API base URL
    - **endpoint**: API endpoint
    - **table_name**: Target table name
    - **auth_type**: Authentication type ('none', 'api_key', 'bearer', etc.)
    - **auth_params**: Authentication parameters
    - **paginated**: Handle pagination
    - **pagination_type**: Pagination type ('offset', 'page', 'cursor')
    - **max_pages**: Maximum pages to fetch
    - **pagination_config**: Pagination configuration
    - **rate_limit_config**: Rate limiting configuration
    """
    logger.info(f"🌐 API ingestion request: {request.table_name}")
    
    try:
        from scripts.Extract import DataExtractor
        from pathlib import Path
        
        extractor = DataExtractor()
        df, stats = extractor.extract_from_api(
            base_url=request.base_url,
            endpoint=request.endpoint,
            table_name=request.table_name,
            auth_type=request.auth_type,
            auth_params=request.auth_params or {},
            paginated=request.paginated,
            pagination_type=request.pagination_type,
            max_pages=request.max_pages,
            pagination_config=request.pagination_config,
            rate_limit_config=request.rate_limit_config
        )
        
        if df is not None:
            # Save to staging
            staging_path = Path(f"data/staging/{request.table_name}_api_extract.csv")
            staging_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(staging_path, index=False)
            
            return {
                "status": "success",
                "table_name": request.table_name,
                "rows_extracted": len(df),
                "columns": len(df.columns),
                "staging_file": str(staging_path),
                "extraction_time_seconds": stats.get('duration_seconds', 0),
                "timestamp": datetime.now().isoformat()
            }
        else:
            raise HTTPException(status_code=500, detail="API ingestion failed")
            
    except Exception as e:
        logger.error(f"❌ API ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════
# FILE WATCHER ENDPOINTS
# ═══════════════════════════════════════════════════════════

@router.get("/file-watcher/status", summary="Get file watcher status")
async def get_file_watcher_status():
    """Get current file watcher status and statistics"""
    logger.info("📊 File watcher status request")
    
    try:
        from scripts.file_watcher import FileWatcher
        
        watcher = FileWatcher(
            watch_dirs=['data/raw/dataset', 'data/raw'],
            file_patterns=['*.csv', '*.json', '*.jsonl'],
            history_file='data/file_watch_history.json'
        )
        
        stats = watcher.get_statistics()
        
        return {
            "status": "active",
            "watch_directories": stats['watch_directories'],
            "file_patterns": stats['file_patterns'],
            "total_processed_files": stats['total_processed_files'],
            "check_interval_seconds": stats['check_interval_seconds'],
            "history_file": stats['history_file'],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ File watcher status error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/file-watcher/scan", summary="Trigger manual file scan")
async def trigger_file_scan():
    """Manually trigger file watcher scan"""
    logger.info("🔍 Manual file scan triggered")
    
    try:
        from scripts.file_watcher import FileWatcher
        
        watcher = FileWatcher(
            watch_dirs=['data/raw/dataset', 'data/raw'],
            file_patterns=['*.csv', '*.json', '*.jsonl'],
            history_file='data/file_watch_history.json'
        )
        
        new_files = watcher.scan_directories()
        
        return {
            "status": "success",
            "new_files_count": len(new_files),
            "files": [
                {
                    "file_name": f['file_name'],
                    "file_path": f['file_path'],
                    "file_size_mb": f['file_size_mb'],
                    "file_extension": f['file_extension'],
                    "detected_time": f['detected_time']
                }
                for f in new_files
            ],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ File scan error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════
# INGESTION HISTORY ENDPOINT
# ═══════════════════════════════════════════════════════════

@router.get("/history", summary="Get ingestion history")
async def get_ingestion_history(limit: int = 50):
    """
    Get ingestion history from database
    
    - **limit**: Maximum number of records to return
    """
    logger.info(f"📋 Ingestion history request (limit: {limit})")
    
    try:
        from scripts.Load import DatabaseLoader
        import pandas as pd
        
        loader = DatabaseLoader()
        
        # Query ingestion_logs table
        query = f"""
        SELECT 
            table_name,
            source_type,
            rows_extracted,
            columns_extracted,
            extraction_time_seconds,
            status,
            extracted_at
        FROM etl_output.ingestion_logs
        ORDER BY extracted_at DESC
        LIMIT {limit}
        """
        
        result = loader.execute_query(query)
        
        if result:
            return {
                "status": "success",
                "count": len(result),
                "records": result.to_dict('records')
            }
        else:
            return {
                "status": "success",
                "count": 0,
                "records": [],
                "message": "No ingestion history found"
            }
            
    except Exception as e:
        logger.error(f"❌ Ingestion history error: {e}")
        # Return empty result if table doesn't exist yet
        return {
            "status": "success",
            "count": 0,
            "records": [],
            "message": f"Ingestion history not available: {str(e)}"
        }
