# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL CUSTOMERS DAG
# Tasks: T0023-T0027 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_customers.py - Customers Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (email callbacks)
- T0025: Multi-DAG dependency management (independent DAG)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Extract → Transform → Load (Customers)
Schedule: Midnight IST (18:30 UTC)
Dependencies: None (Independent)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys
import os
sys.path.insert(0, '/opt/airflow/scripts')

from dag_base import (
    DEFAULT_ARGS, DAG_CONFIG, SCHEDULE_MIDNIGHT_IST, START_DATE,
    send_success_email, send_failure_email,
    DATA_RAW, DATA_STAGING, DATA_PROCESSED, get_connection_string,
    DATA_BRONZE, DATA_SILVER, copy_to_medallion
)


# ========================================
# Team 1 - T0028: Combined E-T-L Pipeline
# ========================================
# ETL TASK FUNCTIONS

def extract_customers(**context):
    """Extract customers data from CSV"""
    import pandas as pd
    from pathlib import Path
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Customers.csv'
    staging_file = f'{DATA_STAGING}/customers_raw.csv'
    
    # Read source
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    # Save to staging
    df.to_csv(staging_file, index=False)

    # Bronze layer copy
    copy_to_medallion(staging_file, DATA_BRONZE, 'customers_raw.csv')
    
    print(f"✅ Extracted {row_count:,} customers to staging")
    
    # Push to XCom
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_customers(**context):
    """Clean and transform customers data"""
    import pandas as pd
    import numpy as np
    import re
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/customers_cleaned.csv'
    
    # Read from staging
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    rejected_records = []
    
    # ========================================
    # Team 1 - T0010: Duplicate data detection & removal
    # ========================================
    duplicates = df[df.duplicated(subset=['CustomerKey'], keep='first')]
    for _, row in duplicates.iterrows():
        rejected_records.append({
            'table': 'customers',
            'record_id': str(row.get('CustomerKey', '')),
            'reason': 'duplicate',
            'original_data': row.to_json()
        })
    df = df.drop_duplicates(subset=['CustomerKey'], keep='first')
    duplicates_removed = initial_rows - len(df)
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (birthday)
    # Birthday: Replace empty values with placeholder date
    # ========================================
    if 'Birthday' in df.columns:
        df['Birthday'] = pd.to_datetime(df['Birthday'], errors='coerce')
        # Replace NaT (null) with placeholder date 1900-01-01
        df['Birthday'] = df['Birthday'].fillna(pd.Timestamp('1900-01-01'))
    
    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # Team 1 - T0011: Missing data handling (mean fill for Age)
    # ========================================
    if 'Age' in df.columns:
        df['Age'] = pd.to_numeric(df['Age'], errors='coerce')
        mean_age = df['Age'].mean()
        df['Age'] = df['Age'].fillna(mean_age).astype(int)
    
    # ========================================
    # Email Validation: Remove invalid emails and track rejections
    # ========================================
    if 'Email' in df.columns:
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        def is_valid_email(email):
            if pd.isna(email) or email == '':
                return False
            return bool(re.match(email_pattern, str(email)))
        
        # Track invalid emails for rejection
        invalid_email_mask = ~df['Email'].apply(is_valid_email)
        invalid_emails = df[invalid_email_mask]
        for _, row in invalid_emails.iterrows():
            rejected_records.append({
                'table': 'customers',
                'record_id': str(row.get('CustomerKey', '')),
                'reason': 'invalid_email',
                'original_data': row.to_json()
            })
        
        # Remove records with invalid emails
        df = df[~invalid_email_mask]
        print(f"   Removed {len(invalid_emails)} records with invalid emails")
    
    # ========================================
    # Customer Loyalty Categorization
    # Based on Age: Premium (50+), Standard (30-49), Basic (<30)
    # ========================================
    def categorize_loyalty(age):
        if pd.isna(age):
            return 'Unknown'
        if age >= 50:
            return 'Premium'
        elif age >= 30:
            return 'Standard'
        else:
            return 'Basic'
    
    df['loyalty_category'] = df['Age'].apply(categorize_loyalty)
    
    # Save cleaned data
    df.to_csv(output_file, index=False)

    # Silver layer copy
    copy_to_medallion(output_file, DATA_SILVER, 'customers_cleaned.csv')
    
    print(f"✅ Transformed customers: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    print(f"   Total rejected: {len(rejected_records)}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    context['ti'].xcom_push(key='rejected_records', value=rejected_records)
    
    return {'rows': len(df), 'file': output_file, 'rejected': len(rejected_records)}


def load_customers(**context):
    """Load customers to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import text
    from Load import DatabaseLoader
    from datetime import datetime
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    rejected_records = context['ti'].xcom_pull(key='rejected_records', task_ids='transform') or []
    
    # Read cleaned data
    df = pd.read_csv(output_file)
    
    loader = DatabaseLoader()
    engine = loader.connect()
    
    # Create schema if not exists (handle race condition)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        except Exception:
            pass  # Schema already exists
        conn.execute(text("DROP TABLE IF EXISTS etl_output.customers"))
        
        # Create rejected_records table if not exists (append-only)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_output.rejected_records (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                record_id VARCHAR(100),
                reason VARCHAR(100),
                original_data TEXT,
                rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                dag_run_id VARCHAR(255)
            )
        """))
        
        # Create dag_run_summary table if not exists (append-only)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_output.dag_run_summary (
                id SERIAL PRIMARY KEY,
                dag_id VARCHAR(100),
                run_id VARCHAR(255),
                execution_date TIMESTAMP,
                table_name VARCHAR(100),
                rows_extracted INT,
                rows_loaded INT,
                rows_rejected INT,
                status VARCHAR(50),
                completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    
    # Load customers to database (governance hooks included)
    stats = loader.load_table(
        'customers_cleaned',
        df,
        mode='replace',
        source_location=output_file,
        context=context
    )
    
    # Save rejected records (append)
    if rejected_records:
        rejected_df = pd.DataFrame(rejected_records)
        rejected_df['dag_run_id'] = context['run_id']
        rejected_df.rename(columns={'table': 'table_name'}, inplace=True)
        rejected_df.to_sql(
            'rejected_records',
            engine,
            schema='etl_output',
            if_exists='append',
            index=False
        )
        print(f"   Saved {len(rejected_records)} rejected records")
    
    # Save dag run summary (append)
    run_summary = pd.DataFrame([{
        'dag_id': 'etl_customers',
        'run_id': context['run_id'],
        'execution_date': str(context['logical_date']),
        'table_name': 'customers',
        'rows_extracted': context['ti'].xcom_pull(key='row_count', task_ids='extract'),
        'rows_loaded': len(df),
        'rows_rejected': len(rejected_records),
        'status': 'success'
    }])
    run_summary.to_sql(
        'dag_run_summary',
        engine,
        schema='etl_output',
        if_exists='append',
        index=False
    )
    
    print(f"✅ Loaded {stats.get('loaded_rows', len(df)):,} customers to etl_output.customers")
    
    context['ti'].xcom_push(key='loaded_rows', value=stats.get('loaded_rows', len(df)))
    
    loader.disconnect()
    return {'rows': stats.get('loaded_rows', len(df)), 'table': 'etl_output.customers'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_customers',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Customers table',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'customers'],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Extract task
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_customers,
        provide_context=True,
    )
    
    # Transform task
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_customers,
        provide_context=True,
    )
    
    # Load task
    load = PythonOperator(
        task_id='load',
        python_callable=load_customers,
        provide_context=True,
    )
    
    # End marker with success callback
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    # Task dependencies
    start >> extract >> transform >> load >> end
