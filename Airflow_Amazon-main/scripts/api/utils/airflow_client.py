# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 7: Airflow Database Client
# Task: T0034
# ═══════════════════════════════════════════════════════════════════════

"""
airflow_client.py - Airflow Metadata Database Client

TASK IMPLEMENTED:
- T0034: Expose pipeline run status (Database queries)

Provides methods for querying Airflow's metadata database:
- List DAGs
- Get DAG runs
- Get task instances
- Get table statistics
"""

from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import json
import pickle
from sqlalchemy import create_engine, text, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

from ..config import config
from ..models.dag_models import DAGInfo, DAGRunInfo, TaskInstanceInfo
from ..models.response_models import TableStats


# ========================================
# Team 1 - T0034: Airflow Database Client Class
# ========================================

class AirflowClient:
    """Client for querying Airflow metadata database"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize Airflow database client.
        
        Args:
            database_url: PostgreSQL connection URL (uses config if None)
        """
        self.database_url = database_url or config.database_url
        self._engine: Optional[Engine] = None
        self._session_maker: Optional[sessionmaker] = None
    
    @property
    def engine(self) -> Engine:
        """Get or create database engine"""
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10
            )
        return self._engine
    
    @property
    def session_maker(self) -> sessionmaker:
        """Get or create session maker"""
        if self._session_maker is None:
            self._session_maker = sessionmaker(bind=self.engine)
        return self._session_maker
    
    def get_session(self) -> Session:
        """Create a new database session"""
        return self.session_maker()
    
    def test_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    # ========================================
    # Team 1 - T0034: DAG Query Methods
    # ========================================
    
    def list_dags(self) -> List[DAGInfo]:
        """
        Get list of all DAGs with latest run information.
        
        Returns:
            List of DAGInfo objects
        """
        query = text("""
            SELECT 
                d.dag_id,
                d.description,
                d.schedule_interval,
                d.is_paused,
                d.is_active,
                d.owners,
                d.last_parsed_time as last_run_date,
                d.next_dagrun as next_run_date,
                dr.run_id,
                dr.state,
                dr.execution_date,
                dr.start_date,
                dr.end_date
            FROM dag d
            LEFT JOIN LATERAL (
                SELECT run_id, state, execution_date, start_date, end_date
                FROM dag_run
                WHERE dag_id = d.dag_id
                ORDER BY execution_date DESC
                LIMIT 1
            ) dr ON true
            WHERE d.is_active = true
            ORDER BY d.dag_id
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query)
            dags = []
            for row in result:
                # Create LatestRunInfo if run data exists
                latest_run = None
                if row.run_id:
                    from ..models.dag_models import LatestRunInfo
                    latest_run = LatestRunInfo(
                        run_id=row.run_id,
                        state=row.state,
                        execution_date=row.execution_date,
                        start_date=row.start_date,
                        end_date=row.end_date
                    )
                
                dags.append(DAGInfo(
                    dag_id=row.dag_id,
                    description=row.description,
                    schedule_interval=row.schedule_interval,
                    is_paused=row.is_paused,
                    is_active=row.is_active,
                    tags=[],  # Would need separate query for tags
                    owners=[row.owners] if row.owners else [],
                    last_run_date=row.last_run_date,
                    next_run_date=row.next_run_date,
                    latest_run=latest_run
                ))
            return dags
    
    def get_dag_info(self, dag_id: str) -> Optional[DAGInfo]:
        """
        Get information for a specific DAG with latest run.
        
        Args:
            dag_id: DAG identifier
            
        Returns:
            DAGInfo object or None if not found
        """
        query = text("""
            SELECT 
                d.dag_id,
                d.description,
                d.schedule_interval,
                d.is_paused,
                d.is_active,
                d.owners,
                d.last_parsed_time as last_run_date,
                d.next_dagrun as next_run_date,
                dr.run_id,
                dr.state,
                dr.execution_date,
                dr.start_date,
                dr.end_date
            FROM dag d
            LEFT JOIN LATERAL (
                SELECT run_id, state, execution_date, start_date, end_date
                FROM dag_run
                WHERE dag_id = d.dag_id
                ORDER BY execution_date DESC
                LIMIT 1
            ) dr ON true
            WHERE d.dag_id = :dag_id
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {"dag_id": dag_id}).fetchone()
            if result:
                # Create LatestRunInfo if run data exists
                latest_run = None
                if result.run_id:
                    from ..models.dag_models import LatestRunInfo
                    latest_run = LatestRunInfo(
                        run_id=result.run_id,
                        state=result.state,
                        execution_date=result.execution_date,
                        start_date=result.start_date,
                        end_date=result.end_date
                    )
                
                return DAGInfo(
                    dag_id=result.dag_id,
                    description=result.description,
                    schedule_interval=result.schedule_interval,
                    is_paused=result.is_paused,
                    is_active=result.is_active,
                    tags=[],
                    owners=[result.owners] if result.owners else [],
                    last_run_date=result.last_run_date,
                    next_run_date=result.next_run_date,
                    latest_run=latest_run
                )
            return None
    
    def get_dag_runs(
        self,
        dag_id: str,
        limit: int = 50,
        offset: int = 0,
        state: Optional[str] = None
    ) -> tuple[List[DAGRunInfo], int]:
        """
        Get DAG runs with pagination.
        
        Args:
            dag_id: DAG identifier
            limit: Maximum number of runs to return
            offset: Number of runs to skip
            state: Filter by state (success, failed, running, etc.)
            
        Returns:
            Tuple of (list of DAGRunInfo, total count)
        """
        # Build WHERE clause
        where_clause = "WHERE dag_id = :dag_id"
        params: Dict[str, Any] = {"dag_id": dag_id, "limit": limit, "offset": offset}
        
        if state:
            where_clause += " AND state = :state"
            params["state"] = state
        
        # Query for runs
        query = text(f"""
            SELECT 
                dag_id,
                run_id,
                execution_date,
                start_date,
                end_date,
                state,
                run_type,
                EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds,
                conf
            FROM dag_run
            {where_clause}
            ORDER BY execution_date DESC
            LIMIT :limit OFFSET :offset
        """)
        
        # Count query
        count_query = text(f"""
            SELECT COUNT(*) as total
            FROM dag_run
            {where_clause}
        """)
        
        with self.engine.connect() as conn:
            # Get total count
            count_result = conn.execute(count_query, params).fetchone()
            total = count_result.total if count_result else 0
            
            # Get runs
            result = conn.execute(query, params)
            runs = []
            for row in result:
                # Handle conf field - Airflow stores it as pickled data
                conf_value = {}
                if row.conf is not None:
                    try:
                        if isinstance(row.conf, (memoryview, bytes)):
                            # Airflow uses pickle for conf
                            conf_value = pickle.loads(bytes(row.conf))
                        elif isinstance(row.conf, dict):
                            conf_value = row.conf
                        elif isinstance(row.conf, str):
                            conf_value = json.loads(row.conf)
                    except Exception:
                        # If parsing fails, use empty dict
                        conf_value = {}
                
                runs.append(DAGRunInfo(
                    dag_id=row.dag_id,
                    run_id=row.run_id,
                    execution_date=row.execution_date,
                    start_date=row.start_date,
                    end_date=row.end_date,
                    state=row.state,
                    run_type=row.run_type,
                    duration_seconds=float(row.duration_seconds) if row.duration_seconds else None,
                    conf=conf_value
                ))
            
            return runs, total
    
    def get_latest_dag_run(self, dag_id: str) -> Optional[DAGRunInfo]:
        """
        Get the most recent DAG run.
        
        Args:
            dag_id: DAG identifier
            
        Returns:
            DAGRunInfo object or None
        """
        runs, _ = self.get_dag_runs(dag_id, limit=1, offset=0)
        return runs[0] if runs else None
    
    def get_dag_run_tasks(self, dag_id: str, run_id: str) -> List[TaskInstanceInfo]:
        """
        Get task instances for a specific DAG run.
        
        Args:
            dag_id: DAG identifier
            run_id: Run identifier
            
        Returns:
            List of TaskInstanceInfo objects
        """
        query = text("""
            SELECT 
                task_id,
                dag_id,
                run_id,
                state,
                start_date,
                end_date,
                EXTRACT(EPOCH FROM (end_date - start_date)) as duration_seconds,
                try_number,
                max_tries
            FROM task_instance
            WHERE dag_id = :dag_id AND run_id = :run_id
            ORDER BY start_date
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {"dag_id": dag_id, "run_id": run_id})
            tasks = []
            for row in result:
                tasks.append(TaskInstanceInfo(
                    task_id=row.task_id,
                    dag_id=row.dag_id,
                    run_id=row.run_id,
                    state=row.state,
                    start_date=row.start_date,
                    end_date=row.end_date,
                    duration_seconds=float(row.duration_seconds) if row.duration_seconds else None,
                    try_number=row.try_number,
                    max_tries=row.max_tries
                ))
            return tasks
    
    def get_dag_run_counts(self, dag_id: str) -> Dict[str, int]:
        """
        Get run counts by state for a DAG.
        
        Args:
            dag_id: DAG identifier
            
        Returns:
            Dictionary with counts (total, success, failed)
        """
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as success,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed
            FROM dag_run
            WHERE dag_id = :dag_id
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {"dag_id": dag_id}).fetchone()
            if result:
                return {
                    "total": result.total or 0,
                    "success": result.success or 0,
                    "failed": result.failed or 0
                }
            return {"total": 0, "success": 0, "failed": 0}
    
    def get_etl_metrics(self) -> List[Dict[str, Any]]:
        """
        Get ETL metrics from dag_run_summary table.
        
        Returns:
            List of dictionaries with table-level metrics
        """
        query = text("""
            SELECT 
                table_name,
                SUM(rows_extracted) as rows_extracted,
                SUM(rows_loaded) as rows_loaded,
                SUM(rows_rejected) as rows_rejected,
                MAX(execution_date) as last_run_date
            FROM etl_output.dag_run_summary
            WHERE table_name IS NOT NULL
            GROUP BY table_name
            ORDER BY table_name
        """)
        
        try:
            print(f"[DEBUG] Connecting to database: {self.engine.url}")
            with self.engine.connect() as conn:
                print("[DEBUG] Executing ETL metrics query...")
                result = conn.execute(query)
                metrics = []
                row_count = 0
                for row in result:
                    row_count += 1
                    metrics.append({
                        "table_name": row.table_name,
                        "rows_extracted": int(row.rows_extracted or 0),
                        "rows_loaded": int(row.rows_loaded or 0),
                        "rows_rejected": int(row.rows_rejected or 0),
                        "last_run_date": row.last_run_date.isoformat() if row.last_run_date else None
                    })
                print(f"[DEBUG] Query returned {row_count} rows")
                print(f"[DEBUG] Metrics: {metrics}")
                return metrics
        except Exception as e:
            print(f"[ERROR] Error fetching ETL metrics: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    # ========================================
    # Team 1 - T0035: Table Statistics Methods
    # ========================================
    
    def get_table_stats(self, schema: str = None) -> List[TableStats]:
        """
        Get statistics for all tables in a schema.
        
        Args:
            schema: Schema name (uses ETL_SCHEMA from config if None)
            
        Returns:
            List of TableStats objects
        """
        schema = schema or config.DB_SCHEMA
        
        query = text("""
            SELECT 
                table_name,
                table_schema as schema_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {"schema": schema})
            stats = []
            
            for row in result:
                # Get row count for each table
                count_query = text(f'SELECT COUNT(*) as cnt FROM "{schema}"."{row.table_name}"')
                count_result = conn.execute(count_query).fetchone()
                row_count = count_result.cnt if count_result else 0
                
                stats.append(TableStats(
                    table_name=row.table_name,
                    schema_name=row.schema_name,
                    row_count=row_count,
                    last_updated=None,
                    size_bytes=None,
                    columns=None
                ))
            
            return stats
    
    def get_runs_today(self) -> Dict[str, int]:
        """
        Get DAG run counts for today.
        
        Returns:
            Dictionary with total, successful, and failed counts
        """
        query = text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) as success,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed
            FROM dag_run
            WHERE DATE(execution_date) = CURRENT_DATE
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query).fetchone()
            if result:
                return {
                    "total": result.total or 0,
                    "success": result.success or 0,
                    "failed": result.failed or 0
                }
            return {"total": 0, "success": 0, "failed": 0}
    
    def close(self):
        """Close database connections"""
        if self._engine:
            self._engine.dispose()
