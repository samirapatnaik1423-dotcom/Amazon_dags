# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 6: DAG Execution Tracker
# Tasks: T0031
# ═══════════════════════════════════════════════════════════════════════

"""
DAG Execution Metadata Tracker
Task: T0031 - Pipeline execution summary

Captures and stores:
- Run ID, DAG name, timestamps
- Task execution details
- Data processing metrics
- Quality scores
- Errors and warnings
"""

import pandas as pd
import sqlite3
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path
import logging
import json

logger = logging.getLogger(__name__)


class DAGExecutionSummary:
    """
    TEAM 1 - T0031: Track and report DAG execution metadata
    """
    
    def __init__(self, dag_name: str, run_id: str):
        """
        Initialize execution tracker
        
        Args:
            dag_name: Name of the DAG
            run_id: Unique run identifier
        """
        self.dag_name = dag_name
        self.run_id = run_id
        self.start_time = datetime.now()
        self.end_time = None
        self.status = 'RUNNING'
        self.metrics = {
            'input_rows_processed': 0,
            'output_tables_generated': 0,
            'data_quality_score': 0.0,
            'errors_logged': 0,
            'total_tasks': 0,
            'tasks_succeeded': 0,
            'tasks_failed': 0
        }
        self.task_details = []
        
        logger.info(f"▶ TEAM 1 - T0031: Initialized execution tracker for {dag_name}")
    
    def record_task(self, task_id: str, status: str, rows_processed: int = 0, duration_seconds: float = 0):
        """Record individual task execution"""
        self.task_details.append({
            'task_id': task_id,
            'status': status,
            'rows_processed': rows_processed,
            'duration_seconds': duration_seconds,
            'timestamp': datetime.now().isoformat()
        })
        
        self.metrics['total_tasks'] += 1
        if status == 'SUCCESS':
            self.metrics['tasks_succeeded'] += 1
        elif status == 'FAILED':
            self.metrics['tasks_failed'] += 1
            self.metrics['errors_logged'] += 1
    
    def update_metrics(self, **kwargs):
        """Update execution metrics"""
        for key, value in kwargs.items():
            if key in self.metrics:
                self.metrics[key] = value
    
    def finalize(self, status: str = 'SUCCESS'):
        """Finalize execution summary"""
        self.end_time = datetime.now()
        self.status = status
        self.metrics['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary"""
        return {
            'Run_ID': self.run_id,
            'DAG_Name': self.dag_name,
            'Start_Time': self.start_time.isoformat(),
            'End_Time': self.end_time.isoformat() if self.end_time else None,
            'Duration_Seconds': self.metrics.get('duration_seconds', 0),
            'Status': self.status,
            'Total_Tasks': self.metrics['total_tasks'],
            'Tasks_Succeeded': self.metrics['tasks_succeeded'],
            'Tasks_Failed': self.metrics['tasks_failed'],
            'Input_Rows_Processed': self.metrics['input_rows_processed'],
            'Output_Tables_Generated': self.metrics['output_tables_generated'],
            'Data_Quality_Score': self.metrics['data_quality_score'],
            'Errors_Logged': self.metrics['errors_logged'],
            'Trigger_Type': 'manual',  # Can be enhanced
            'Executed_By': 'airflow_scheduler',
            'Task_Details': json.dumps(self.task_details)
        }
    
    def save_to_csv(self, output_dir: Path):
        """
        Save execution summary to CSV (append mode)
        
        Args:
            output_dir: Directory to save summary CSV
        """
        logger.info("▶ TEAM 1 - T0031: Saving execution summary to CSV")
        
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / 'dag_execution_summary.csv'
        
        # Convert to DataFrame
        summary_df = pd.DataFrame([self.to_dict()])
        
        # Append to existing file or create new
        if csv_path.exists():
            existing_df = pd.read_csv(csv_path)
            summary_df = pd.concat([existing_df, summary_df], ignore_index=True)
        
        summary_df.to_csv(csv_path, index=False)
        logger.info(f"✅ Execution summary saved to {csv_path}")
    
    def save_to_database(self, db_path: str):
        """
        Save execution summary to SQLite database
        
        Args:
            db_path: Path to SQLite database
        """
        logger.info("▶ TEAM 1 - T0031: Saving execution summary to database")
        
        try:
            conn = sqlite3.connect(db_path)
            
            # Create table if not exists
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS dag_execution_metadata (
                Run_ID TEXT PRIMARY KEY,
                DAG_Name TEXT,
                Start_Time TEXT,
                End_Time TEXT,
                Duration_Seconds REAL,
                Status TEXT,
                Total_Tasks INTEGER,
                Tasks_Succeeded INTEGER,
                Tasks_Failed INTEGER,
                Input_Rows_Processed INTEGER,
                Output_Tables_Generated INTEGER,
                Data_Quality_Score REAL,
                Errors_Logged INTEGER,
                Trigger_Type TEXT,
                Executed_By TEXT,
                Task_Details TEXT
            )
            """
            conn.execute(create_table_sql)
            
            # Insert summary
            summary_df = pd.DataFrame([self.to_dict()])
            summary_df.to_sql('dag_execution_metadata', conn, if_exists='append', index=False)
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ Execution summary saved to database: {db_path}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save to database: {e}")
            raise
    
    def print_summary(self):
        """Print human-readable summary"""
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - T0031: DAG EXECUTION SUMMARY")
        logger.info("="*70)
        logger.info(f"Run ID:              {self.run_id}")
        logger.info(f"DAG Name:            {self.dag_name}")
        logger.info(f"Start Time:          {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if self.end_time:
            logger.info(f"End Time:            {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Duration:            {self.metrics['duration_seconds']:.2f} seconds")
        logger.info(f"Status:              {self.status}")
        logger.info(f"Total Tasks:         {self.metrics['total_tasks']}")
        logger.info(f"  - Succeeded:       {self.metrics['tasks_succeeded']}")
        logger.info(f"  - Failed:          {self.metrics['tasks_failed']}")
        logger.info(f"Rows Processed:      {self.metrics['input_rows_processed']:,}")
        logger.info(f"Tables Generated:    {self.metrics['output_tables_generated']}")
        logger.info(f"Data Quality Score:  {self.metrics['data_quality_score']:.1f}%")
        logger.info(f"Errors Logged:       {self.metrics['errors_logged']}")
        logger.info("="*70 + "\n")


def calculate_data_quality_score(cleaning_stats: Dict[str, Dict[str, Any]]) -> float:
    """
    Calculate overall data quality score (0-100)
    
    Factors:
    - Duplicate removal rate
    - Missing data handling
    - Validation success rate
    
    Args:
        cleaning_stats: Dictionary with cleaning statistics
    
    Returns:
        Quality score (0-100)
    """
    total_score = 0
    num_tables = len(cleaning_stats)
    
    for table_name, stats in cleaning_stats.items():
        table_score = 100.0
        
        # Deduct for duplicates (up to -10 points)
        input_rows = stats.get('input_rows', 1)
        duplicates = stats.get('duplicates_removed', 0)
        dup_rate = (duplicates / input_rows) * 100
        table_score -= min(dup_rate, 10)
        
        # Deduct for missing data (up to -10 points)
        missing_filled = stats.get('missing_ages_filled', 0) + stats.get('missing_dates_filled', 0)
        missing_rate = (missing_filled / input_rows) * 100
        table_score -= min(missing_rate / 2, 10)  # Less penalty for filling
        
        # Bonus for validation fixes (+5 points)
        if stats.get('invalid_emails_fixed', 0) > 0:
            table_score = min(table_score + 5, 100)
        
        total_score += max(table_score, 0)
    
    return round(total_score / num_tables, 1)
