"""
ETL JSON Ingestion DAG
TEAM 2 - SPRINT 1 (PHASE 1)
DAG for ingesting data from JSON/JSONL files
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

def extract_from_json(**context):
    """Extract data from JSON file"""
    logger.info("📖 Starting JSON extraction...")
    
    # Import here to avoid DAG import timeout
    from scripts.Extract import DataExtractor
    extractor = DataExtractor()
    
    # Check for JSON files in data/raw directory
    json_files = list(Path("data/raw").glob("*.json")) + list(Path("data/raw").glob("*.jsonl"))
    
    if not json_files:
        logger.warning("⚠️ No JSON files found in data/raw/")
        # Create sample JSON for demo
        import json
        sample_data = [
            {"id": 1, "name": "Sample Product 1", "price": 99.99, "category": "Electronics"},
            {"id": 2, "name": "Sample Product 2", "price": 49.99, "category": "Books"},
            {"id": 3, "name": "Sample Product 3", "price": 29.99, "category": "Clothing"}
        ]
        sample_file = Path("data/raw/sample_products.json")
        sample_file.parent.mkdir(parents=True, exist_ok=True)
        with open(sample_file, 'w') as f:
            json.dump(sample_data, f, indent=2)
        json_files = [sample_file]
        logger.info(f"✅ Created sample JSON file: {sample_file}")
    
    # Extract first JSON file
    json_file = json_files[0]
    is_jsonl = json_file.suffix == '.jsonl'
    
    df, stats = extractor.extract_json(
        file_path=json_file,
        table_name='json_data',
        normalize=True,
        is_jsonl=is_jsonl
    )
    
    if df is not None:
        # Save to staging
        staging_path = Path("data/staging/json_data_raw.csv")
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(staging_path, index=False)
        
        logger.info(f"✅ JSON extraction complete: {len(df)} records from {json_file.name}")
        
        return {
            'table_name': 'json_data',
            'source_file': str(json_file),
            'rows_extracted': len(df),
            'columns': len(df.columns),
            'staging_file': str(staging_path),
            'status': 'success'
        }
    else:
        raise RuntimeError("JSON extraction failed")

def load_to_database(**context):
    """Load JSON data to database"""
    logger.info("💾 Loading JSON data to database...")
    
    ti = context['ti']
    extract_info = ti.xcom_pull(task_ids='extract_from_json')
    
    if not extract_info:
        raise RuntimeError("No extraction data found in XCom")
    
    # Load data
    from sqlalchemy import create_engine
    import pandas as pd
    
    AIRFLOW_DB = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(AIRFLOW_DB)
    
    df = pd.read_csv(extract_info['staging_file'])
    
    # Save to database (etl_output.json_data)
    df.to_sql(
        name='json_data',
        con=engine,
        schema='etl_output',
        if_exists='replace',
        index=False
    )
    rows_loaded = len(df)
    engine.dispose()
    
    logger.info(f"✅ Loaded {rows_loaded} records to database")
    
    return {
        'table_name': 'json_data',
        'rows_loaded': rows_loaded,
        'status': 'success'
    }

# Create DAG
with DAG(
    dag_id='etl_json_ingestion',
    default_args=default_args,
    description='Ingest data from JSON/JSONL files (PHASE 1)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['phase1', 'json', 'ingestion', 'team2']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract_json = PythonOperator(
        task_id='extract_from_json',
        python_callable=extract_from_json,
        provide_context=True
    )
    
    load_db = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task flow
    start >> extract_json >> load_db >> end
