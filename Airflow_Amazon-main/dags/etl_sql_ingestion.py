"""
ETL SQL Ingestion DAG
TEAM 2 - SPRINT 1 (PHASE 1)
DAG for extracting data from SQL databases
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

def extract_from_sql(**context):
    """Extract data from SQL database"""
    logger.info("🗄️ Starting SQL extraction...")
    
    # Import here to avoid DAG import timeout
    from scripts.Extract import DataExtractor
    extractor = DataExtractor()
    
    # Connection string for local PostgreSQL (same as Airflow DB)
    connection_string = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    
    # Example: Extract data from etl_output.customers table
    df, stats = extractor.extract_from_sql(
        connection_string=connection_string,
        table_name='customers',
        schema='etl_output'
    )
    
    if df is not None:
        # Save to staging
        staging_path = Path("data/staging/sql_extract_customers.csv")
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(staging_path, index=False)
        
        logger.info(f"✅ SQL extraction complete: {len(df)} records")
        
        return {
            'table_name': 'sql_extract_customers',
            'source': 'etl_output.customers',
            'rows_extracted': len(df),
            'columns': len(df.columns),
            'staging_file': str(staging_path),
            'status': 'success'
        }
    else:
        raise RuntimeError("SQL extraction failed")

def execute_sql_transformation(**context):
    """Execute SQL transformation using templates"""
    logger.info("🔄 Executing SQL transformation...")
    
    # Import SQL transformation engine
    from scripts.sql_transformations.engine import SQLTransformationEngine
    
    connection_string = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = SQLTransformationEngine(connection_string)
    
    # Example: Aggregate sales by store using direct SQL query
    try:
        # Use execute_query for full control over SQL
        query = """
        SELECT 
            "StoreKey",
            SUM("Quantity") as total_quantity,
            COUNT("Order Number") as order_count
        FROM etl_output.sales
        GROUP BY "StoreKey"
        ORDER BY "StoreKey"
        """
        
        result = engine.execute_query(query)
        
        # Save result to staging
        staging_path = Path("data/staging/sales_by_store.csv")
        engine.save_to_csv(result, staging_path)
        
        logger.info(f"✅ Transformation complete: {len(result)} records")
        
        engine.close()
        
        return {
            'transformation': 'sales_by_store_aggregate',
            'rows_generated': len(result),
            'staging_file': str(staging_path),
            'status': 'success'
        }
    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        engine.close()
        raise

def load_to_database(**context):
    """Load transformed data to database"""
    logger.info("💾 Loading SQL transformation results to database...")
    
    ti = context['ti']
    transform_info = ti.xcom_pull(task_ids='execute_sql_transformation')
    
    if not transform_info:
        logger.warning("⚠️ No transformation data found, skipping load")
        return {'status': 'skipped'}
    
    # Load data
    from sqlalchemy import create_engine
    import pandas as pd
    
    AIRFLOW_DB = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    engine = create_engine(AIRFLOW_DB)
    
    df = pd.read_csv(transform_info['staging_file'])
    
    # Save to database (etl_output.sales_by_store)
    df.to_sql(
        name='sales_by_store',
        con=engine,
        schema='etl_output',
        if_exists='replace',
        index=False
    )
    rows_loaded = len(df)
    engine.dispose()
    
    logger.info(f"✅ Loaded {rows_loaded} records to database")
    
    return {
        'table_name': 'sales_by_store',
        'rows_loaded': rows_loaded,
        'status': 'success'
    }

# Create DAG
with DAG(
    dag_id='etl_sql_ingestion',
    default_args=default_args,
    description='Extract from SQL databases and execute SQL transformations (PHASE 1)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['phase1', 'sql', 'ingestion', 'transformation', 'team2', 'team3']
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract_sql = PythonOperator(
        task_id='extract_from_sql',
        python_callable=extract_from_sql,
        provide_context=True
    )
    
    transform_sql = PythonOperator(
        task_id='execute_sql_transformation',
        python_callable=execute_sql_transformation,
        provide_context=True
    )
    
    load_db = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end')
    
    # Task flow
    start >> extract_sql >> transform_sql >> load_db >> end
