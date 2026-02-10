# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: DAG Status Endpoints
# Task: T0034
# ═══════════════════════════════════════════════════════════════════════

"""
dags.py - DAG status and run endpoints

TASK IMPLEMENTED:
- T0034: Expose pipeline run status

Provides endpoints for:
- Listing all DAGs
- Getting DAG status
- Getting DAG runs (paginated)
- Getting specific run details
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Depends, status

from ..auth import get_api_key
from ..models.dag_models import (
    DAGInfo,
    DAGRunInfo,
    DAGStatusResponse,
    DAGRunsResponse
)
from ..models.response_models import ErrorResponse
from ..utils.airflow_client import AirflowClient
from ..utils.pagination import calculate_offset_limit


# ========================================
# Team 1 - T0034: DAG Router Configuration
# ========================================

router = APIRouter(
    prefix="/api/v1/dags",
    tags=["DAGs"],
    dependencies=[Depends(get_api_key)],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"},
        404: {"model": ErrorResponse, "description": "Not Found"}
    }
)


# ========================================
# Team 1 - T0034: List All DAGs Endpoint
# ========================================

@router.get(
    "",
    response_model=List[DAGInfo],
    summary="List DAGs",
    description="Get list of all active DAGs"
)
async def list_dags(
    api_key: str = Depends(get_api_key)
):
    """
    Get list of all DAGs.
    
    Returns:
        List of DAGInfo objects
    """
    client = AirflowClient()
    dags = client.list_dags()
    return dags


# ========================================
# Team 1 - T0034: Get DAG Status Endpoint
# ========================================

@router.get(
    "/{dag_id}/status",
    response_model=DAGStatusResponse,
    summary="Get DAG Status",
    description="Get current status and latest run for a specific DAG"
)
async def get_dag_status(
    dag_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Get DAG status with latest run information.
    
    Args:
        dag_id: DAG identifier
        
    Returns:
        DAGStatusResponse with current status
        
    Raises:
        HTTPException: 404 if DAG not found
    """
    client = AirflowClient()
    
    # Get DAG info
    dag_info = client.get_dag_info(dag_id)
    if not dag_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG '{dag_id}' not found"
        )
    
    # Get latest run
    latest_run = client.get_latest_dag_run(dag_id)
    
    # Get task instances if there's a latest run
    task_instances = []
    if latest_run:
        task_instances = client.get_dag_run_tasks(dag_id, latest_run.run_id)
    
    # Get run counts
    counts = client.get_dag_run_counts(dag_id)
    
    return DAGStatusResponse(
        dag_info=dag_info,
        latest_run=latest_run,
        task_instances=task_instances,
        total_runs=counts["total"],
        success_count=counts["success"],
        failed_count=counts["failed"]
    )


# ========================================
# Team 1 - T0034: Get DAG Runs Endpoint (Paginated)
# ========================================

@router.get(
    "/{dag_id}/runs",
    response_model=DAGRunsResponse,
    summary="Get DAG Runs",
    description="Get paginated list of DAG runs with optional state filter"
)
async def get_dag_runs(
    dag_id: str,
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    page_size: int = Query(50, ge=1, le=500, description="Items per page"),
    state: Optional[str] = Query(None, description="Filter by state (success, failed, running)"),
    api_key: str = Depends(get_api_key)
):
    """
    Get paginated list of DAG runs.
    
    Args:
        dag_id: DAG identifier
        page: Page number (1-indexed)
        page_size: Number of items per page
        state: Optional state filter
        
    Returns:
        DAGRunsResponse with paginated runs
        
    Raises:
        HTTPException: 404 if DAG not found
    """
    client = AirflowClient()
    
    # Verify DAG exists
    dag_info = client.get_dag_info(dag_id)
    if not dag_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG '{dag_id}' not found"
        )
    
    # Calculate pagination
    offset, limit = calculate_offset_limit(page, page_size)
    
    # Get runs
    runs, total = client.get_dag_runs(dag_id, limit=limit, offset=offset, state=state)
    
    # Calculate if there are more pages
    has_next = (offset + limit) < total
    
    return DAGRunsResponse(
        dag_id=dag_id,
        runs=runs,
        total=total,
        page=page,
        page_size=page_size,
        has_next=has_next
    )


# ========================================
# Team 1 - T0034: Get Specific DAG Run Endpoint
# ========================================

@router.get(
    "/{dag_id}/runs/{run_id}",
    response_model=DAGRunInfo,
    summary="Get DAG Run Details",
    description="Get detailed information for a specific DAG run"
)
async def get_dag_run(
    dag_id: str,
    run_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Get details for a specific DAG run.
    
    Args:
        dag_id: DAG identifier
        run_id: Run identifier
        
    Returns:
        DAGRunInfo object
        
    Raises:
        HTTPException: 404 if DAG or run not found
    """
    client = AirflowClient()
    
    # Get all runs and find the specific one
    runs, _ = client.get_dag_runs(dag_id, limit=1000, offset=0)
    
    for run in runs:
        if run.run_id == run_id:
            return run
    
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Run '{run_id}' not found for DAG '{dag_id}'"
    )


# ========================================
# Team 1 - T0034: Get DAG Run Tasks Endpoint
# ========================================

@router.get(
    "/{dag_id}/runs/{run_id}/tasks",
    response_model=List[DAGRunInfo],
    summary="Get DAG Run Tasks",
    description="Get all task instances for a specific DAG run"
)
async def get_dag_run_tasks(
    dag_id: str,
    run_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    Get task instances for a DAG run.
    
    Args:
        dag_id: DAG identifier
        run_id: Run identifier
        
    Returns:
        List of TaskInstanceInfo objects
        
    Raises:
        HTTPException: 404 if DAG or run not found
    """
    client = AirflowClient()
    
    # Verify DAG and run exist
    dag_info = client.get_dag_info(dag_id)
    if not dag_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG '{dag_id}' not found"
        )
    
    # Get tasks
    tasks = client.get_dag_run_tasks(dag_id, run_id)
    
    if not tasks:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No tasks found for run '{run_id}'"
        )
    
    return tasks
