# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Metadata Summary Endpoint
# Task: T0035
# ═══════════════════════════════════════════════════════════════════════

"""
metadata.py - Pipeline metadata endpoints

TASK IMPLEMENTED:
- T0035: Expose metadata summary

Provides endpoints for:
- Pipeline metadata summary
- Table statistics
- ETL metrics
"""

from datetime import datetime
from typing import List
from fastapi import APIRouter, Depends, status

from ..auth import get_api_key
from ..models.response_models import MetadataSummary, TableStats, ErrorResponse
from ..utils.airflow_client import AirflowClient
from ..config import config


# ========================================
# Team 1 - T0035: Metadata Router Configuration
# ========================================

router = APIRouter(
    prefix="/api/v1/metadata",
    tags=["Metadata"],
    dependencies=[Depends(get_api_key)],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"}
    }
)


# ========================================
# Team 1 - T0035: Get Metadata Summary Endpoint
# ========================================

@router.get(
    "/summary",
    response_model=MetadataSummary,
    summary="Get Metadata Summary",
    description="Get comprehensive pipeline metadata including DAG stats and table info"
)
async def get_metadata_summary(
    api_key: str = Depends(get_api_key)
):
    """
    Get complete pipeline metadata summary.
    
    Returns:
        MetadataSummary with DAG statistics and table information
    """
    client = AirflowClient()
    
    # Get all DAGs
    dags = client.list_dags()
    
    # Count active and paused DAGs
    active_dags = sum(1 for dag in dags if not dag.is_paused)
    paused_dags = sum(1 for dag in dags if dag.is_paused)
    
    # Get today's run statistics
    today_runs = client.get_runs_today()
    
    # Get table statistics
    tables = client.get_table_stats(schema=config.DB_SCHEMA)
    
    return MetadataSummary(
        pipeline_name=config.APP_NAME,
        total_dags=len(dags),
        active_dags=active_dags,
        paused_dags=paused_dags,
        total_runs_today=today_runs["total"],
        successful_runs_today=today_runs["success"],
        failed_runs_today=today_runs["failed"],
        tables=tables,
        last_refresh=datetime.now()
    )


# ========================================
# Team 1 - T0035: Get Table Statistics Endpoint
# ========================================

@router.get(
    "/tables",
    response_model=List[TableStats],
    summary="Get Table Statistics",
    description="Get statistics for all ETL output tables"
)
async def get_table_statistics(
    api_key: str = Depends(get_api_key)
):
    """
    Get statistics for all tables in the ETL schema.
    
    Returns:
        List of TableStats objects
    """
    client = AirflowClient()
    tables = client.get_table_stats(schema=config.DB_SCHEMA)
    return tables


# ========================================
# Team 1 - T0035: Get Pipeline Metrics Endpoint  
# ========================================

@router.get(
    "/metrics",
    summary="Get Pipeline Metrics",
    description="Get ETL metrics with rows extracted, loaded, and rejected per table"
)
async def get_pipeline_metrics(
    api_key: str = Depends(get_api_key)
):
    """
    Get ETL metrics from dag_run_summary table - RETURNS ARRAY DIRECTLY.
    """
    print("=" * 80)
    print("[METRICS] Endpoint called!")
    print("=" * 80)
    
    # Return the actual ETL metrics from get_etl_metrics
    client = AirflowClient()
    metrics = client.get_etl_metrics()
    
    print(f"[METRICS] Returning: {metrics}")
    return metrics
