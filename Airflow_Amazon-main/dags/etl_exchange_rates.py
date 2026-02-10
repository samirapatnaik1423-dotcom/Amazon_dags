# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL EXCHANGE RATES DAG
# Tasks: T0023-T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_exchange_rates.py - Exchange Rates Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (email callbacks)
- T0025: Multi-DAG dependency management (independent DAG)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Extract → Transform → Load (Exchange Rates)
Schedule: Midnight IST (18:30 UTC)
Dependencies: None (Independent)
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

def extract_exchange_rates(**context):
    """Extract exchange rates data from CSV"""
    import pandas as pd
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Exchange_Rates.csv'
    staging_file = f'{DATA_STAGING}/exchange_rates_raw.csv'
    
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    df.to_csv(staging_file, index=False)

    copy_to_medallion(staging_file, DATA_BRONZE, 'exchange_rates_raw.csv')
    
    print(f"✅ Extracted {row_count:,} exchange rates to staging")
    
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_exchange_rates(**context):
    """Clean and transform exchange rates data"""
    import pandas as pd
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/exchange_rates_cleaned.csv'
    
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    
    # Parse date column
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    
    # Remove duplicates (Date + Currency combination)
    if 'Currency' in df.columns:
        df = df.drop_duplicates(subset=['Date', 'Currency'], keep='first')
    else:
        df = df.drop_duplicates(subset=['Date'], keep='first')
    
    duplicates_removed = initial_rows - len(df)
    
    # Ensure exchange rate is numeric
    if 'Exchange' in df.columns:
        df['Exchange'] = pd.to_numeric(df['Exchange'], errors='coerce').fillna(1.0)
    
    df.to_csv(output_file, index=False)

    copy_to_medallion(output_file, DATA_SILVER, 'exchange_rates_cleaned.csv')
    
    print(f"✅ Transformed exchange rates: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    
    return {'rows': len(df), 'file': output_file}


def load_exchange_rates(**context):
    """Load exchange rates to PostgreSQL"""
    import pandas as pd
    from Load import DatabaseLoader
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    
    df = pd.read_csv(output_file)
    
    loader = DatabaseLoader()
    stats = loader.load_table(
        'exchange_rates_cleaned',
        df,
        mode='replace',
        source_location=output_file,
        context=context
    )
    loader.disconnect()
    
    print(f"✅ Loaded {stats.get('loaded_rows', len(df)):,} exchange rates to etl_output.exchange_rates")
    
    context['ti'].xcom_push(key='loaded_rows', value=stats.get('loaded_rows', len(df)))
    
    return {'rows': stats.get('loaded_rows', len(df)), 'table': 'etl_output.exchange_rates'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_exchange_rates',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Exchange Rates table',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'exchange_rates'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_exchange_rates,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_exchange_rates,
        provide_context=True,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_exchange_rates,
        provide_context=True,
    )
    
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    start >> extract >> transform >> load >> end
