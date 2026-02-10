# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL DAG BASE MODULE
# Tasks: T0023, T0026, T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
dag_base.py - Shared DAG Configuration and Utilities

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines - Shared configuration
- T0026: Backfill & catchup features - catchup=True, max_active_runs=3
- T0027: DAG failure handling strategy - Email callbacks, retries

Provides:
- Common DAG default arguments (retries, timeouts, email)
- Email notification settings (success/failure callbacks)
- Shared utility functions for all ETL DAGs
- Database connection utilities
- Path constants for data directories
"""

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.sensors.external_task import ExternalTaskSensor
from typing import Optional, List
import os
import shutil

# ========================================
# DAG DEFAULT ARGUMENTS
# ========================================

# Email for notifications
ALERT_EMAIL = os.environ.get('SMTP_USER', 'samirapatnaik1423@gmail.com')

# ========================================
# Team 1 - T0032: Error Recovery Workflow
# ========================================
# Common default args for all DAGs (retries, timeouts)
DEFAULT_ARGS = {
    'owner': 'team1',
    'depends_on_past': False,
    'email': [ALERT_EMAIL],
    'email_on_failure': False,  # Disabled - enable after SMTP setup
    'email_on_retry': False,    # Disabled to reduce email noise
    'retries': 3,
    'retry_delay': timedelta(minutes=1),  # Reduced for testing
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(minutes=int(os.getenv('SLA_MINUTES', '60'))),
    'sla_miss_callback': None,
}


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis, *args, **kwargs):
    """Send email on SLA miss."""
    subject = f"⏰ SLA Miss: {dag.dag_id}"
    task_names = ", ".join(task_list or [])
    body = f"""
    <h2>SLA Miss Detected</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag.dag_id}</td></tr>
        <tr><td><b>Tasks</b></td><td>{task_names}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{slas[0].execution_date if slas else 'N/A'}</td></tr>
    </table>
    <p>Please review task durations and upstream dependencies.</p>
    """
    send_email(to=ALERT_EMAIL, subject=subject, html_content=body)


# Attach SLA callback after definition
DEFAULT_ARGS['sla_miss_callback'] = sla_miss_callback

# Schedule: Midnight IST = 18:30 UTC
# IST is UTC+5:30, so midnight IST = 18:30 previous day UTC
SCHEDULE_MIDNIGHT_IST = '30 18 * * *'

# Start date for catchup
START_DATE = datetime(2025, 12, 25)

# ========================================
# Team 1 - T0026: Backfill & Catchup Features
# ========================================
# Advanced backfill controls
BACKFILL_ENABLED = os.getenv('BACKFILL_ENABLED', 'true').lower() == 'true'
BACKFILL_MAX_ACTIVE_RUNS = int(os.getenv('BACKFILL_MAX_ACTIVE_RUNS', '3'))
BACKFILL_WINDOW_DAYS = int(os.getenv('BACKFILL_WINDOW_DAYS', '14'))

# Common DAG configuration
DAG_CONFIG = {
    'start_date': START_DATE,
    'schedule_interval': SCHEDULE_MIDNIGHT_IST,
    'catchup': BACKFILL_ENABLED,
    'max_active_runs': BACKFILL_MAX_ACTIVE_RUNS,
    'tags': ['team1', 'etl', 'amazon'],
}


# ========================================
# Team 1 - T0027: DAG Failure Handling Strategy
# ========================================
# EMAIL NOTIFICATION FUNCTIONS
# ========================================

def send_success_email(context):
    """Send email on successful DAG completion"""
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    
    subject = f"✅ ETL Success: {dag_id}"
    body = f"""
    <h2>ETL Pipeline Completed Successfully</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Status</b></td><td style="color:green;">SUCCESS</td></tr>
    </table>
    <p>All tasks completed without errors.</p>
    """
    
    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=body
    )


def send_failure_email(context):
    """Send email on task failure"""
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    
    subject = f"❌ ETL Failed: {dag_id} - {task_id}"
    body = f"""
    <h2>ETL Pipeline Failed</h2>
    <table border="1" cellpadding="5">
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Status</b></td><td style="color:red;">FAILED</td></tr>
        <tr><td><b>Error</b></td><td>{exception}</td></tr>
    </table>
    <p>Please check the logs for more details.</p>
    """
    
    send_email(
        to=ALERT_EMAIL,
        subject=subject,
        html_content=body
    )


# ========================================
# DATA PATHS
# ========================================

# Base paths (relative to Airflow container)
DATA_RAW = '/opt/airflow/data/raw/dataset'
DATA_STAGING = '/opt/airflow/data/staging'
DATA_PROCESSED = '/opt/airflow/data/processed'
DATA_REPORTS = '/opt/airflow/data/reports'

# Phase 5 - Medallion Architecture
DATA_BRONZE = '/opt/airflow/data/bronze'
DATA_SILVER = '/opt/airflow/data/silver'
DATA_GOLD = '/opt/airflow/data/gold'
DATA_ARCHIVE = '/opt/airflow/data/archive'


def copy_to_medallion(source_path: str, stage_dir: str, target_name: str) -> str:
    """Copy a file into the medallion layer folder."""
    os.makedirs(stage_dir, exist_ok=True)
    target_path = os.path.join(stage_dir, target_name)
    shutil.copy2(source_path, target_path)
    return target_path


def build_external_task_sensor(
    task_id: str,
    external_dag_id: str,
    external_task_id: Optional[str] = None,
    timeout: int = 900,
    poke_interval: int = 5,
    allowed_states: Optional[List[str]] = None,
    failed_states: Optional[List[str]] = None,
    mode: str = "reschedule",
):
    """Standardize cross-DAG dependency sensors. Optimized for fast execution."""
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=allowed_states or ["success"],
        failed_states=failed_states or ["failed", "skipped"],
        timeout=timeout,
        poke_interval=poke_interval,
        mode=mode,
    )

# ========================================
# Team 1 - T0029: Multi-Source Data Pipelines
# ========================================
# Source files
SOURCE_FILES = {
    'customers': f'{DATA_RAW}/Customers.csv',
    'sales': f'{DATA_RAW}/Sales.csv',
    'products': f'{DATA_RAW}/Products.csv',
    'stores': f'{DATA_RAW}/Stores.csv',
    'exchange_rates': f'{DATA_RAW}/Exchange_Rates.csv',
}

# Processed files
PROCESSED_FILES = {
    'customers': f'{DATA_PROCESSED}/customers_cleaned.csv',
    'sales': f'{DATA_PROCESSED}/sales_cleaned.csv',
    'products': f'{DATA_PROCESSED}/products_cleaned.csv',
    'stores': f'{DATA_PROCESSED}/stores_cleaned.csv',
    'exchange_rates': f'{DATA_PROCESSED}/exchange_rates_cleaned.csv',
}


# ========================================
# DATABASE CONFIGURATION
# ========================================

DB_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'schema': 'etl_output'
}

def get_connection_string():
    """Get PostgreSQL connection string for ETL tables"""
    return (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )


# ========================================
# Team 1 - T0030: Reusable Pipeline Config
# ========================================
# XCOM KEYS FOR INTER-DAG COMMUNICATION

XCOM_KEYS = {
    'extraction_complete': 'extraction_complete',
    'transformation_complete': 'transformation_complete',
    'load_complete': 'load_complete',
    'row_count': 'row_count',
    'output_file': 'output_file',
}
