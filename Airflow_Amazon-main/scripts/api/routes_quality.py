# ═══════════════════════════════════════════════════════════════════════
# PHASE 1 - TEAM 2 SPRINT 2: Quality & Validation API Routes
# Task: T0012 - Quality Scorecards & API Integration
# ═══════════════════════════════════════════════════════════════════════

"""
Quality and validation API endpoints

Endpoints:
- GET /quality/scorecard - Current quality scores for all tables
- GET /quality/scorecard/{table} - Quality score for specific table
- GET /quality/validation-results - Latest validation results
- GET /quality/anomalies - Detected anomalies
- GET /quality/profiles - Data profiling information
- GET /quality/trends - Historical quality trends
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import json

logger = logging.getLogger(__name__)

from .config import config

# Database connection
engine = create_engine(config.database_url)


def _raise_if_missing_table(error: Exception, table_name: str) -> None:
    message = str(error)
    if "does not exist" in message or "UndefinedTable" in message:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found. Run etl_data_quality DAG first."
        )

router = APIRouter(prefix="/quality", tags=["quality"])


@router.get("/scorecard", summary="Get quality scorecards for all tables")
async def get_quality_scorecard():
    """
    Get the latest quality scorecard showing overall quality scores
    for all tables
    """
    try:
        query = """
        SELECT 
            table_name,
            row_count,
            overall_score,
            completeness_score,
            accuracy_score,
            total_checks,
            passed_checks,
            failed_checks,
            status,
            generated_at
        FROM etl_output.quality_scorecards
        ORDER BY table_name
        """
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {
                "message": "No quality scorecards found. Run etl_data_quality DAG first.",
                "scorecards": []
            }
        
        # Calculate summary statistics
        avg_score = float(df['overall_score'].mean())
        total_checks = int(df['total_checks'].sum())
        total_failed = int(df['failed_checks'].sum())
        
        return {
            "summary": {
                "average_quality_score": round(avg_score, 2),
                "total_checks": total_checks,
                "total_passed": total_checks - total_failed,
                "total_failed": total_failed,
                "tables_analyzed": len(df)
            },
            "scorecards": df.to_dict('records')
        }
    
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.quality_scorecards")
        logger.error(f"Failed to fetch quality scorecard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scorecard/{table_name}", summary="Get quality scorecard for specific table")
async def get_table_scorecard(table_name: str):
    """Get quality scorecard for a specific table"""
    try:
        query = text("""
        SELECT *
        FROM etl_output.quality_scorecards
        WHERE table_name = :table_name
        """)
        
        df = pd.read_sql(query, engine, params={"table_name": table_name})
        
        if df.empty:
            raise HTTPException(status_code=404, 
                              detail=f"No scorecard found for table: {table_name}")
        
        return df.to_dict('records')[0]
    
    except HTTPException:
        raise
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.quality_scorecards")
        logger.error(f"Failed to fetch scorecard for {table_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validation-results", summary="Get latest validation results")
async def get_validation_results(
    table: Optional[str] = Query(None, description="Filter by table name"),
    failed_only: bool = Query(False, description="Show only failed validations")
):
    """Get column-level validation results"""
    try:
        query = "SELECT * FROM etl_output.validation_results"
        conditions = []
        
        if table:
            conditions.append(f"table_name = '{table}'")
        if failed_only:
            conditions.append("is_valid = false")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY table_name, column_name"
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {
                "message": "No validation results found",
                "total_validations": 0,
                "results": []
            }
        
        failed_count = len(df[df['is_valid'] == False])
        
        return {
            "summary": {
                "total_validations": len(df),
                "passed": len(df) - failed_count,
                "failed": failed_count,
                "pass_rate": round(((len(df) - failed_count) / len(df)) * 100, 2) if len(df) > 0 else 0
            },
            "results": df.to_dict('records')
        }
    
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.validation_results")
        logger.error(f"Failed to fetch validation results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies", summary="Get detected anomalies")
async def get_anomalies(
    table: Optional[str] = Query(None, description="Filter by table name"),
    column: Optional[str] = Query(None, description="Filter by column name"),
    limit: int = Query(100, description="Maximum number of anomalies to return")
):
    """Get detected anomalies using Z-score method"""
    try:
        query = "SELECT * FROM etl_output.detected_anomalies"
        conditions = []
        
        if table:
            conditions.append(f"table_name = '{table}'")
        if column:
            conditions.append(f"column_name = '{column}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += f" ORDER BY detected_at DESC, ABS(z_score) DESC LIMIT {limit}"
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {
                "message": "No anomalies detected",
                "total_anomalies": 0,
                "anomalies": []
            }
        
        # Group by table and column for summary
        table_summary = df.groupby('table_name').size().to_dict()
        
        return {
            "summary": {
                "total_anomalies": len(df),
                "by_table": {str(k): int(v) for k, v in table_summary.items()}
            },
            "anomalies": df.head(limit).to_dict('records')
        }
    
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.detected_anomalies")
        logger.error(f"Failed to fetch anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles", summary="Get data profiling information")
async def get_data_profiles(table: Optional[str] = Query(None, description="Filter by table name")):
    """Get statistical data profiles for tables"""
    try:
        query = "SELECT * FROM etl_output.data_profiles"
        
        if table:
            query += f" WHERE table_name = '{table}'"
        
        query += " ORDER BY table_name"
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {
                "message": "No data profiles found. Run etl_data_quality DAG first.",
                "profiles": []
            }
        
        # Parse JSON profiles
        profiles = []
        for _, row in df.iterrows():
            profile = json.loads(row['profile_json'])
            profiles.append(profile)
        
        return {
            "total_tables": len(profiles),
            "profiles": profiles
        }
    
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.data_profiles")
        logger.error(f"Failed to fetch data profiles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality-checks", summary="Get quality rule check results")
async def get_quality_checks(
    table: Optional[str] = Query(None, description="Filter by table name"),
    failed_only: bool = Query(False, description="Show only failed checks")
):
    """Get quality rule execution results"""
    try:
        query = "SELECT * FROM etl_output.quality_check_results"
        conditions = []
        
        if table:
            conditions.append(f"table_name = '{table}'")
        if failed_only:
            conditions.append("passed = false")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY severity DESC, table_name, rule_type"
        
        df = pd.read_sql(query, engine)
        
        if df.empty:
            return {
                "message": "No quality check results found",
                "checks": []
            }
        
        # Summary by severity
        severity_summary = df.groupby('severity').agg({
            'passed': lambda x: x.sum(),
            'rule_type': 'count'
        }).to_dict()
        
        return {
            "summary": {
                "total_checks": len(df),
                "passed": int(df['passed'].sum()),
                "failed": len(df) - int(df['passed'].sum()),
                "by_severity": severity_summary
            },
            "checks": df.to_dict('records')
        }
    
    except Exception as e:
        _raise_if_missing_table(e, "etl_output.quality_check_results")
        logger.error(f"Failed to fetch quality checks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", summary="Get comprehensive quality summary")
async def get_quality_summary():
    """
    Get comprehensive summary of all quality metrics:
    - Scorecard overview
    - Validation status
    - Anomaly counts
    - Quality check results
    """
    try:
        summary = {
            "timestamp": datetime.now().isoformat(),
            "scorecards": {},
            "validations": {},
            "anomalies": {},
            "quality_checks": {}
        }
        
        # Get scorecards
        try:
            scorecard_df = pd.read_sql("SELECT * FROM etl_output.quality_scorecards", engine)
            if not scorecard_df.empty:
                summary["scorecards"] = {
                    "average_score": float(scorecard_df['overall_score'].mean()),
                    "tables_count": len(scorecard_df),
                    "passing_tables": len(scorecard_df[scorecard_df['status'] == 'PASS'])
                }
        except:
            pass
        
        # Get validation results
        try:
            validation_df = pd.read_sql("SELECT * FROM etl_output.validation_results", engine)
            if not validation_df.empty:
                summary["validations"] = {
                    "total": len(validation_df),
                    "passed": int(validation_df['is_valid'].sum()),
                    "failed": len(validation_df) - int(validation_df['is_valid'].sum())
                }
        except:
            pass
        
        # Get anomalies
        try:
            anomaly_df = pd.read_sql("SELECT COUNT(*) as count FROM etl_output.detected_anomalies", engine)
            summary["anomalies"] = {
                "total_detected": int(anomaly_df['count'].iloc[0]) if not anomaly_df.empty else 0
            }
        except:
            pass
        
        # Get quality checks
        try:
            checks_df = pd.read_sql("SELECT * FROM etl_output.quality_check_results", engine)
            if not checks_df.empty:
                summary["quality_checks"] = {
                    "total": len(checks_df),
                    "passed": int(checks_df['passed'].sum()),
                    "failed": len(checks_df) - int(checks_df['passed'].sum())
                }
        except:
            pass
        
        return summary
    
    except Exception as e:
        logger.error(f"Failed to generate quality summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))
