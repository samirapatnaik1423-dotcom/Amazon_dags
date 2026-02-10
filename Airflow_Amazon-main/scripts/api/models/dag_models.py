# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: DAG Data Models
# Task: T0034
# ═══════════════════════════════════════════════════════════════════════

"""
dag_models.py - Pydantic models for DAG-related data

TASK IMPLEMENTED:
- T0034: Expose pipeline run status (Data models)

Provides schema definitions for:
- DAG information
- DAG run details
- Task instance information
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ========================================
# Team 1 - T0034: DAG Information Model
# ========================================

class LatestRunInfo(BaseModel):
    """Latest run summary for a DAG"""
    run_id: str = Field(..., description="Run identifier")
    state: str = Field(..., description="Run state")
    execution_date: datetime = Field(..., description="Execution timestamp")
    start_date: Optional[datetime] = Field(None, description="Start time")
    end_date: Optional[datetime] = Field(None, description="End time")


class DAGInfo(BaseModel):
    """Basic DAG information"""
    dag_id: str = Field(..., description="Unique DAG identifier")
    description: Optional[str] = Field(None, description="DAG description")
    schedule_interval: Optional[str] = Field(None, description="Cron schedule or None")
    is_paused: bool = Field(..., description="Whether DAG is paused")
    is_active: bool = Field(..., description="Whether DAG is active")
    tags: List[str] = Field(default_factory=list, description="DAG tags")
    owners: List[str] = Field(default_factory=list, description="DAG owners")
    last_run_date: Optional[datetime] = Field(None, description="Last execution date")
    next_run_date: Optional[datetime] = Field(None, description="Next scheduled run")
    latest_run: Optional[LatestRunInfo] = Field(None, description="Latest run information")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "etl_customers",
                "description": "Customers Table ETL Pipeline",
                "schedule_interval": "30 18 * * *",
                "is_paused": False,
                "is_active": True,
                "tags": ["team1", "etl"],
                "owners": ["team1"],
                "last_run_date": "2026-01-19T18:30:00",
                "next_run_date": "2026-01-20T18:30:00"
            }
        }


# ========================================
# Team 1 - T0034: DAG Run Information Model
# ========================================

class DAGRunInfo(BaseModel):
    """DAG run execution details"""
    dag_id: str = Field(..., description="DAG identifier")
    run_id: str = Field(..., description="Unique run identifier")
    execution_date: datetime = Field(..., description="Execution timestamp")
    start_date: Optional[datetime] = Field(None, description="Run start time")
    end_date: Optional[datetime] = Field(None, description="Run end time")
    state: str = Field(..., description="Run state (success, failed, running)")
    run_type: str = Field(..., description="Run type (scheduled, manual, backfill)")
    duration_seconds: Optional[float] = Field(None, description="Run duration in seconds")
    conf: Optional[Dict[str, Any]] = Field(None, description="Run configuration")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "etl_customers",
                "run_id": "manual__2026-01-19T10:00:00+00:00",
                "execution_date": "2026-01-19T10:00:00",
                "start_date": "2026-01-19T10:00:05",
                "end_date": "2026-01-19T10:02:30",
                "state": "success",
                "run_type": "manual",
                "duration_seconds": 145.5,
                "conf": {}
            }
        }


# ========================================
# Team 1 - T0034: Task Instance Information Model
# ========================================

class TaskInstanceInfo(BaseModel):
    """Task instance execution details"""
    task_id: str = Field(..., description="Task identifier")
    dag_id: str = Field(..., description="Parent DAG identifier")
    run_id: str = Field(..., description="Parent run identifier")
    state: Optional[str] = Field(None, description="Task state")
    start_date: Optional[datetime] = Field(None, description="Task start time")
    end_date: Optional[datetime] = Field(None, description="Task end time")
    duration_seconds: Optional[float] = Field(None, description="Task duration")
    try_number: int = Field(..., description="Current attempt number")
    max_tries: int = Field(..., description="Maximum retry attempts")
    
    class Config:
        json_schema_extra = {
            "example": {
                "task_id": "extract",
                "dag_id": "etl_customers",
                "run_id": "manual__2026-01-19T10:00:00+00:00",
                "state": "success",
                "start_date": "2026-01-19T10:00:05",
                "end_date": "2026-01-19T10:00:45",
                "duration_seconds": 40.2,
                "try_number": 1,
                "max_tries": 3
            }
        }


# ========================================
# Team 1 - T0034: DAG Status Response Model
# ========================================

class DAGStatusResponse(BaseModel):
    """Complete DAG status with latest run"""
    dag_info: DAGInfo = Field(..., description="DAG metadata")
    latest_run: Optional[DAGRunInfo] = Field(None, description="Most recent run")
    task_instances: List[TaskInstanceInfo] = Field(
        default_factory=list,
        description="Tasks from latest run"
    )
    total_runs: int = Field(..., description="Total number of runs")
    success_count: int = Field(..., description="Successful runs count")
    failed_count: int = Field(..., description="Failed runs count")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dag_info": {
                    "dag_id": "etl_customers",
                    "description": "Customers ETL",
                    "is_paused": False
                },
                "latest_run": {
                    "run_id": "manual__2026-01-19T10:00:00+00:00",
                    "state": "success"
                },
                "task_instances": [],
                "total_runs": 45,
                "success_count": 42,
                "failed_count": 3
            }
        }


# ========================================
# Team 1 - T0034: DAG Runs List Response Model
# ========================================

class DAGRunsResponse(BaseModel):
    """Paginated list of DAG runs"""
    dag_id: str = Field(..., description="DAG identifier")
    runs: List[DAGRunInfo] = Field(..., description="List of DAG runs")
    total: int = Field(..., description="Total number of runs")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Items per page")
    has_next: bool = Field(..., description="Whether more pages exist")
    
    class Config:
        json_schema_extra = {
            "example": {
                "dag_id": "etl_customers",
                "runs": [],
                "total": 45,
                "page": 1,
                "page_size": 50,
                "has_next": False
            }
        }
