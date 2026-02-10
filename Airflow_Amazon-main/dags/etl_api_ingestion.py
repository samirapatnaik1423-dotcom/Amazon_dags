"""
ETL API Ingestion DAG
TEAM 2 - SPRINT 1 (PHASE 1)
DAG for ingesting data from REST APIs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def extract_from_api(**context):
    """Extract data from public API (JSONPlaceholder for demo)"""
    logger.info("🌐 Starting API extraction...")
    
    # Import here to avoid DAG import timeout
    from scripts.Extract import DataExtractor
    extractor = DataExtractor()
    
    # Example: Extract posts from JSONPlaceholder API
    df, stats = extractor.extract_from_api(
        base_url="https://jsonplaceholder.typicode.com",
        endpoint="/posts",
        table_name="api_posts",
        auth_type='none',
        paginated=False
    )
    
    if df is not None:
        # Save to staging
        staging_path = Path("data/staging/api_posts_raw.csv")
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(staging_path, index=False)
        
        logger.info(f"✅ API extraction complete: {len(df)} records")
        
        return {
            'table_name': 'api_posts',
            'rows_extracted': len(df),
            'columns': len(df.columns),
            'staging_file': str(staging_path),
            'status': 'success'
        }
    else:
        raise RuntimeError("API extraction failed")

def load_to_database(**context):
    """Load API data to database"""
    logger.info("💾 Loading API data to database...")
    
    ti = context['ti']
    extract_info = ti.xcom_pull(task_ids='extract_from_api')
    
    if not extract_info:
        raise RuntimeError("No extraction data found in XCom")
    
    # Load data
    from sqlalchemy import create_engine
    import pandas as pd
    
    AIRFLOW_DB = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(AIRFLOW_DB)
    
    df = pd.read_csv(extract_info['staging_file'])
    
    # Save to database (etl_output.api_posts)
    df.to_sql(
        name='api_posts',
        con=engine,
        schema='etl_output',
        if_exists='replace',
        index=False
    )
    rows_loaded = len(df)
    engine.dispose()
    
    logger.info(f"✅ Loaded {rows_loaded} records to database")
    
    return {
        'table_name': 'api_posts',
        'rows_loaded': rows_loaded,
        'status': 'success'
    }

# Create DAG
with DAG(
    dag_id='etl_api_ingestion',
    default_args=default_args,
    description='Ingest data from REST APIs (PHASE 1)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['phase1', 'api', 'ingestion', 'team2']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract_api = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_from_api,
        provide_context=True
    )
    
    load_db = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task flow
    start >> extract_api >> load_db >> end
