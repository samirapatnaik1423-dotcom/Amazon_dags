"""
ETL File Watcher DAG
TEAM 2 - SPRINT 1 (PHASE 1)
DAG for monitoring directories and auto-processing new files
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# IMPORTANT: Do NOT import heavy modules at top level to avoid DAG import timeout
# Import them inside functions instead
import logging

logger = logging.getLogger(__name__)

# DAG configuration
default_args = {
    'owner': 'team2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def scan_for_new_files(**context):
    """Scan directories for new files"""
    logger.info("👀 Scanning for new files...")
    
    # Import here to avoid DAG import timeout
    from scripts.file_watcher import FileWatcher
    
    # Initialize file watcher
    watcher = FileWatcher(
        watch_dirs=['data/raw/dataset', 'data/raw'],
        file_patterns=['*.csv', '*.json', '*.jsonl'],
        check_interval=10,
        history_file='data/file_watch_history.json'
    )
    
    # Scan for new files
    new_files = watcher.scan_directories()
    
    if new_files:
        logger.info(f"🆕 Found {len(new_files)} new file(s):")
        for file_info in new_files:
            logger.info(f"   - {file_info['file_name']} ({file_info['file_size_mb']:.2f} MB)")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='new_files', value=new_files)
        
        return {
            'new_files_count': len(new_files),
            'files': [f['file_name'] for f in new_files],
            'status': 'success'
        }
    else:
        logger.info("ℹ️ No new files detected")
        return {
            'new_files_count': 0,
            'files': [],
            'status': 'no_new_files'
        }

def process_new_files(**context):
    """Process detected new files"""
    logger.info("🔄 Processing new files...")
    
    ti = context['ti']
    scan_result = ti.xcom_pull(task_ids='scan_for_new_files')
    
    if not scan_result or scan_result['new_files_count'] == 0:
        logger.info("ℹ️ No files to process")
        return {'status': 'no_files_to_process'}
    
    new_files = ti.xcom_pull(task_ids='scan_for_new_files', key='new_files')
    
    processed_count = 0
    for file_info in new_files:
        file_path = Path(file_info['file_path'])
        file_ext = file_info['file_extension']
        
        logger.info(f"📋 Processing: {file_path.name}")
        
        # Route to appropriate processing based on file type
        if file_ext in ['.csv']:
            # Trigger CSV processing DAG
            logger.info(f"   → Routing to CSV ETL pipeline")
            # In production, use TriggerDagRunOperator
            
        elif file_ext in ['.json', '.jsonl']:
            # Trigger JSON processing DAG
            logger.info(f"   → Routing to JSON ingestion DAG")
            # In production, use TriggerDagRunOperator
        
        # Mark as processed
        from scripts.file_watcher import FileWatcher
        watcher = FileWatcher(
            watch_dirs=['data/raw/dataset', 'data/raw'],
            file_patterns=['*.csv', '*.json', '*.jsonl'],
            history_file='data/file_watch_history.json'
        )
        watcher.mark_as_processed(file_info['file_path'], file_info['file_hash'])
        
        processed_count += 1
    
    logger.info(f"✅ Processed {processed_count} file(s)")
    
    return {
        'processed_count': processed_count,
        'status': 'success'
    }

def get_watcher_statistics(**context):
    """Get file watcher statistics"""
    logger.info("📊 Getting file watcher statistics...")
    
    # Import here to avoid DAG import timeout
    from scripts.file_watcher import FileWatcher
    
    watcher = FileWatcher(
        watch_dirs=['data/raw/dataset', 'data/raw'],
        file_patterns=['*.csv', '*.json', '*.jsonl'],
        history_file='data/file_watch_history.json'
    )
    
    stats = watcher.get_statistics()
    
    logger.info(f"📋 Statistics:")
    logger.info(f"   - Watch directories: {len(stats['watch_directories'])}")
    logger.info(f"   - File patterns: {stats['file_patterns']}")
    logger.info(f"   - Total processed files: {stats['total_processed_files']}")
    
    return stats

# Create DAG
with DAG(
    dag_id='etl_file_watcher',
    default_args=default_args,
    description='Monitor directories and auto-process new files (PHASE 1)',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['phase1', 'file-watcher', 'monitoring', 'team2']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    scan_files = PythonOperator(
        task_id='scan_for_new_files',
        python_callable=scan_for_new_files,
        provide_context=True
    )
    
    process_files = PythonOperator(
        task_id='process_new_files',
        python_callable=process_new_files,
        provide_context=True
    )
    
    get_stats = PythonOperator(
        task_id='get_watcher_statistics',
        python_callable=get_watcher_statistics,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task flow
    start >> scan_files >> process_files >> get_stats >> end
