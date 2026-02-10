# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Log Retrieval Endpoint
# Task: T0036
# ═══════════════════════════════════════════════════════════════════════

"""
logs.py - Log retrieval endpoints

TASK IMPLEMENTED:
- T0036: Fetch logs via API

Provides endpoints for:
- Fetching DAG logs
- Fetching task logs
- Listing available log files
"""

import os
from pathlib import Path
from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends, status

from ..auth import get_api_key
from ..models.response_models import ErrorResponse
from ..config import config
from ..utils.airflow_client import AirflowClient


# ========================================
# Team 1 - T0036: Logs Router Configuration
# ========================================

router = APIRouter(
    prefix="/api/v1/logs",
    tags=["Logs"],
    dependencies=[Depends(get_api_key)],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        403: {"model": ErrorResponse, "description": "Forbidden"},
        404: {"model": ErrorResponse, "description": "Log file not found"}
    }
)


# ========================================
# Team 1 - T0036: Helper Function to Read Log File
# ========================================

def read_log_file(log_path: Path, max_size: int = None) -> str:
    """
    Read log file content with size limit.
    
    Args:
        log_path: Path to log file
        max_size: Maximum characters to read (None for all)
        
    Returns:
        Log file content
        
    Raises:
        FileNotFoundError: If log file doesn't exist
    """
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")
    
    if not log_path.is_file():
        raise ValueError(f"Path is not a file: {log_path}")
    
    max_size = max_size or config.MAX_LOG_SIZE
    
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read(max_size)
    
    return content


# ========================================
# Team 1 - T0036: Get DAG Logs Endpoint
# ========================================

@router.get(
    "/dags/{dag_id}/runs/{run_id}",
    summary="Get DAG Run Logs",
    description="Fetch logs for a specific DAG run"
)
async def get_dag_run_logs(
    dag_id: str,
    run_id: str,
    max_lines: Optional[int] = Query(None, ge=1, le=10000, description="Maximum number of lines to return"),
    api_key: str = Depends(get_api_key)
):
    """
    Get logs for a specific DAG run.
    
    Args:
        dag_id: DAG identifier
        run_id: Run identifier
        max_lines: Maximum number of log lines to return
        
    Returns:
        Dictionary with log content
        
    Raises:
        HTTPException: 404 if logs not found
    """
    # Verify DAG exists
    client = AirflowClient()
    dag_info = client.get_dag_info(dag_id)
    if not dag_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG '{dag_id}' not found"
        )
    
    # Construct log path
    logs_dir = Path(config.LOGS_DIR)
    dag_log_dir = logs_dir / f"dag_id={dag_id}" / f"run_id={run_id}"
    
    if not dag_log_dir.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No logs found for DAG run '{run_id}'"
        )
    
    # Find all log files in the run directory
    log_files = list(dag_log_dir.rglob("*.log"))
    
    if not log_files:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No log files found for DAG run '{run_id}'"
        )
    
    # Read all log files
    logs = {}
    for log_file in log_files:
        task_name = log_file.parent.name
        try:
            content = read_log_file(log_file)
            if max_lines:
                lines = content.split('\n')
                content = '\n'.join(lines[:max_lines])
            logs[task_name] = content
        except Exception as e:
            logs[task_name] = f"Error reading log: {str(e)}"
    
    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "log_count": len(logs),
        "logs": logs
    }


# ========================================
# Team 1 - T0036: Get Task Logs Endpoint
# ========================================

@router.get(
    "/dags/{dag_id}/runs/{run_id}/tasks/{task_id}",
    summary="Get Task Logs",
    description="Fetch logs for a specific task instance"
)
async def get_task_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = Query(1, ge=1, description="Task attempt number"),
    max_size: Optional[int] = Query(None, ge=100, le=1000000, description="Maximum characters to return"),
    api_key: str = Depends(get_api_key)
):
    """
    Get logs for a specific task instance.
    
    Args:
        dag_id: DAG identifier
        run_id: Run identifier
        task_id: Task identifier
        try_number: Task attempt number
        max_size: Maximum characters to return
        
    Returns:
        Dictionary with log content
        
    Raises:
        HTTPException: 404 if logs not found
    """
    # Construct log path
    # Airflow log structure: logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/attempt={try_number}.log
    logs_dir = Path(config.LOGS_DIR)
    log_file = logs_dir / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={try_number}.log"
    
    try:
        content = read_log_file(log_file, max_size=max_size)
        
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "try_number": try_number,
            "log_size": len(content),
            "content": content,
            "truncated": len(content) >= (max_size or config.MAX_LOG_SIZE)
        }
    
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Log file not found for task '{task_id}' (attempt {try_number})"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading log file: {str(e)}"
        )


# ========================================
# Team 1 - T0036: List Available Logs Endpoint
# ========================================

@router.get(
    "/dags/{dag_id}",
    summary="List Available Logs",
    description="List all available log files for a DAG"
)
async def list_dag_logs(
    dag_id: str,
    api_key: str = Depends(get_api_key)
):
    """
    List all available log files for a DAG.
    
    Args:
        dag_id: DAG identifier
        
    Returns:
        Dictionary with list of available runs and their logs
    """
    # Verify DAG exists
    client = AirflowClient()
    dag_info = client.get_dag_info(dag_id)
    if not dag_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DAG '{dag_id}' not found"
        )
    
    # Find log directory
    logs_dir = Path(config.LOGS_DIR)
    dag_log_dir = logs_dir / f"dag_id={dag_id}"
    
    if not dag_log_dir.exists():
        return {
            "dag_id": dag_id,
            "runs": [],
            "message": "No logs found for this DAG"
        }
    
    # List all run directories
    run_dirs = [d for d in dag_log_dir.iterdir() if d.is_dir() and d.name.startswith("run_id=")]
    
    runs_info = []
    for run_dir in sorted(run_dirs, reverse=True):
        run_id = run_dir.name.replace("run_id=", "")
        
        # Count log files in this run
        log_files = list(run_dir.rglob("*.log"))
        
        runs_info.append({
            "run_id": run_id,
            "log_count": len(log_files),
            "tasks": [f.parent.name for f in log_files]
        })
    
    return {
        "dag_id": dag_id,
        "total_runs": len(runs_info),
        "runs": runs_info[:50]  # Limit to 50 most recent
    }
