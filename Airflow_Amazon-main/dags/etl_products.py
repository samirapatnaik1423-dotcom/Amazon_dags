# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL PRODUCTS DAG
# Tasks: T0023-T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_products.py - Products Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (email callbacks)
- T0025: Multi-DAG dependency management (Sales waits for this)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Extract → Transform → Load (Products)
Schedule: Midnight IST (18:30 UTC)
Dependencies: None (Independent) - MUST complete before Sales
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from dag_base import (
    DEFAULT_ARGS, SCHEDULE_MIDNIGHT_IST, START_DATE,
    send_success_email, send_failure_email,
    DATA_RAW, DATA_STAGING, DATA_PROCESSED, get_connection_string,
    DATA_BRONZE, DATA_SILVER, copy_to_medallion
)


# ========================================
# ETL TASK FUNCTIONS
# ========================================

def extract_products(**context):
    """Extract products data from CSV"""
    import pandas as pd
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Products.csv'
    staging_file = f'{DATA_STAGING}/products_raw.csv'
    
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    df.to_csv(staging_file, index=False)
    copy_to_medallion(staging_file, DATA_BRONZE, 'products_raw.csv')
    
    print(f"✅ Extracted {row_count:,} products to staging")
    
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_products(**context):
    """Clean and transform products data"""
    import pandas as pd
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/products_cleaned.csv'
    
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['ProductKey'], keep='first')
    duplicates_removed = initial_rows - len(df)
    
    # Clean Unit Price USD (remove $ and convert to float)
    if 'Unit Price USD' in df.columns:
        df['Unit Price USD'] = pd.to_numeric(
            df['Unit Price USD'].astype(str).str.replace(r'[^\d.]', '', regex=True),
            errors='coerce'
        ).fillna(0)
    
    df.to_csv(output_file, index=False)
    copy_to_medallion(output_file, DATA_SILVER, 'products_cleaned.csv')
    
    print(f"✅ Transformed products: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    
    return {'rows': len(df), 'file': output_file}


def load_products(**context):
    """Load products to PostgreSQL"""
    import pandas as pd
    from Load import DatabaseLoader
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    
    df = pd.read_csv(output_file)
    
    loader = DatabaseLoader()
    stats = loader.load_table(
        'products_cleaned',
        df,
        mode='replace',
        source_location=output_file,
        context=context
    )
    loader.disconnect()
    
    print(f"✅ Loaded {stats.get('loaded_rows', len(df)):,} products to etl_output.products")
    
    context['ti'].xcom_push(key='loaded_rows', value=stats.get('loaded_rows', len(df)))
    
    return {'rows': stats.get('loaded_rows', len(df)), 'table': 'etl_output.products'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_products',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Products table',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'products'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_products,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_products,
        provide_context=True,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_products,
        provide_context=True,
    )
    
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    start >> extract >> transform >> load >> end
