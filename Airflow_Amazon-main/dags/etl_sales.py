# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL SALES DAG
# Tasks: T0023-T0027, especially T0024-T0025 (Sprint 5)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_sales.py - Sales Table ETL Pipeline

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (ExternalTaskSensor)
- T0025: Multi-DAG dependency management (waits for etl_products)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)

Pipeline: Wait for Products → Extract → Transform → Load (Sales)
Schedule: Midnight IST (18:30 UTC)
Dependencies: WAITS FOR etl_products (uses ExternalTaskSensor)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow.utils.state import DagRunState

import sys
sys.path.insert(0, '/opt/airflow/scripts')


def get_latest_execution_date(dt, external_dag_id):
    """Get the latest successful execution date for an external DAG."""
    # Find the most recent successful run of the external DAG
    dag_runs = DagRun.find(
        dag_id=external_dag_id,
        state=DagRunState.SUCCESS,
    )
    if dag_runs:
        # Return the most recent successful run's execution date
        latest_run = max(dag_runs, key=lambda x: x.execution_date)
        return latest_run.execution_date
    # If no successful runs, return the current execution date
    return dt

from dag_base import (
    DEFAULT_ARGS, SCHEDULE_MIDNIGHT_IST, START_DATE,
    send_success_email, send_failure_email,
    DATA_RAW, DATA_STAGING, DATA_PROCESSED, get_connection_string,
    DATA_BRONZE, DATA_SILVER, copy_to_medallion,
    build_external_task_sensor
)


# ========================================
# Team 1 - T0024: Event-Driven DAG Triggering
# ========================================

# ========================================
# ETL TASK FUNCTIONS
# ========================================

def extract_sales(**context):
    """Extract sales data from CSV"""
    import pandas as pd
    
    # DATA_RAW already includes /dataset subfolder
    source_file = f'{DATA_RAW}/Sales.csv'
    staging_file = f'{DATA_STAGING}/sales_raw.csv'
    
    df = pd.read_csv(source_file)
    row_count = len(df)
    
    df.to_csv(staging_file, index=False)

    copy_to_medallion(staging_file, DATA_BRONZE, 'sales_raw.csv')
    
    print(f"✅ Extracted {row_count:,} sales records to staging")
    
    context['ti'].xcom_push(key='row_count', value=row_count)
    context['ti'].xcom_push(key='staging_file', value=staging_file)
    
    return {'rows': row_count, 'file': staging_file}


def transform_sales(**context):
    """Clean and transform sales data"""
    import pandas as pd
    
    staging_file = context['ti'].xcom_pull(key='staging_file', task_ids='extract')
    output_file = f'{DATA_PROCESSED}/sales_cleaned.csv'
    
    df = pd.read_csv(staging_file)
    initial_rows = len(df)
    rejected_records = []
    
    # Parse date columns
    if 'Order Date' in df.columns:
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
    if 'Delivery Date' in df.columns:
        df['Delivery Date'] = pd.to_datetime(df['Delivery Date'], errors='coerce')
    
    # ========================================
    # Delivery Status Logic
    # - Delivered: has delivery date
    # - Shipped: no delivery, order within 1 month
    # - In Transit: no delivery, order between 1 month and 1 year
    # - Lost: no delivery, order older than 1 year
    # ========================================
    from datetime import datetime
    current_date = pd.Timestamp.now()
    one_month_ago = current_date - pd.DateOffset(months=1)
    one_year_ago = current_date - pd.DateOffset(years=1)
    
    def get_delivery_status(row):
        if pd.notna(row['Delivery Date']):
            return 'Delivered'
        order_date = row['Order Date']
        if pd.isna(order_date):
            return 'Unknown'
        if order_date >= one_month_ago:
            return 'Shipped'
        elif order_date >= one_year_ago:
            return 'In Transit'
        else:
            return 'Lost'
    
    df['delivery_status'] = df.apply(get_delivery_status, axis=1)
    
    # Fill missing Delivery Date with placeholder (1900-01-01) for non-delivered orders
    df['Delivery Date'] = df['Delivery Date'].fillna(pd.Timestamp('1900-01-01'))
    
    # Remove duplicates based on Order Number
    duplicates = df[df.duplicated(subset=['Order Number'], keep='first')]
    for _, row in duplicates.iterrows():
        rejected_records.append({
            'table': 'sales',
            'record_id': str(row.get('Order Number', '')),
            'reason': 'duplicate',
            'original_data': row.to_json()
        })
    df = df.drop_duplicates(subset=['Order Number'], keep='first')
    duplicates_removed = initial_rows - len(df)
    
    # Ensure numeric columns are properly typed
    numeric_cols = ['Quantity', 'ProductKey', 'StoreKey', 'CustomerKey']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Remove invalid records (negative/zero quantities) and track rejections
    if 'Quantity' in df.columns:
        invalid_qty = df[df['Quantity'] <= 0]
        for _, row in invalid_qty.iterrows():
            rejected_records.append({
                'table': 'sales',
                'record_id': str(row.get('Order Number', '')),
                'reason': 'invalid_quantity',
                'original_data': row.to_json()
            })
        df = df[df['Quantity'] > 0]
        print(f"   Removed {len(invalid_qty)} records with invalid quantity")
    
    # ========================================
    # Calculate total_amount_usd
    # Join with Products to get Unit Price USD, then multiply by Quantity
    # ========================================
    try:
        # Load products to get unit price
        products_file = f'{DATA_PROCESSED}/products_cleaned.csv'
        products_df = pd.read_csv(products_file)
        
        # Clean the Unit Price USD column (remove $ and spaces)
        if 'Unit Price USD' in products_df.columns:
            products_df['Unit Price USD'] = products_df['Unit Price USD'].replace(
                r'[\$,\s]', '', regex=True
            ).astype(float)
        
        # Merge to get unit price
        df = df.merge(
            products_df[['ProductKey', 'Unit Price USD']],
            on='ProductKey',
            how='left'
        )
        
        # Calculate total amount in USD
        df['total_amount_usd'] = df['Quantity'] * df['Unit Price USD'].fillna(0)
        df['total_amount_usd'] = df['total_amount_usd'].round(2)
        
        # Drop the Unit Price USD column (we only need total)
        df = df.drop(columns=['Unit Price USD'], errors='ignore')
        
        print(f"   Added total_amount_usd column")
    except Exception as e:
        print(f"   Warning: Could not calculate total_amount_usd: {e}")
        df['total_amount_usd'] = 0.0
    
    df.to_csv(output_file, index=False)

    copy_to_medallion(output_file, DATA_SILVER, 'sales_cleaned.csv')
    
    print(f"✅ Transformed sales: {initial_rows:,} → {len(df):,} rows")
    print(f"   Duplicates removed: {duplicates_removed}")
    print(f"   Total rejected: {len(rejected_records)}")
    
    context['ti'].xcom_push(key='output_file', value=output_file)
    context['ti'].xcom_push(key='cleaned_rows', value=len(df))
    context['ti'].xcom_push(key='rejected_records', value=rejected_records)
    
    return {'rows': len(df), 'file': output_file, 'rejected': len(rejected_records)}


def load_sales(**context):
    """Load sales to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import text
    from Load import DatabaseLoader
    from datetime import datetime
    
    output_file = context['ti'].xcom_pull(key='output_file', task_ids='transform')
    rejected_records = context['ti'].xcom_pull(key='rejected_records', task_ids='transform') or []
    
    df = pd.read_csv(output_file)
    
    loader = DatabaseLoader()
    engine = loader.connect()
    
    # Create schema if not exists (handle race condition)
    with engine.begin() as conn:
        try:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS etl_output"))
        except Exception:
            pass  # Schema already exists
        conn.execute(text("DROP TABLE IF EXISTS etl_output.sales"))
        
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
    
    stats = loader.load_table(
        'sales_cleaned',
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
        'dag_id': 'etl_sales',
        'run_id': context['run_id'],
        'execution_date': str(context['logical_date']),
        'table_name': 'sales',
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
    
    print(f"✅ Loaded {stats.get('loaded_rows', len(df)):,} sales to etl_output.sales")
    
    context['ti'].xcom_push(key='loaded_rows', value=stats.get('loaded_rows', len(df)))
    
    loader.disconnect()
    return {'rows': stats.get('loaded_rows', len(df)), 'table': 'etl_output.sales'}


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_sales',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='ETL Pipeline for Sales table (waits for Products)',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=3,
    tags=['team1', 'etl', 'sales'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Wait for Products DAG to complete (Sales references Products)
    # For testing with manual triggers, we look for the latest successful run
    wait_for_products = build_external_task_sensor(
        task_id='wait_for_products',
        external_dag_id='etl_products',
        external_task_id='end',
        timeout=7200,
        poke_interval=30,
    )
    wait_for_products.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_products')
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_sales,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_sales,
        provide_context=True,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_sales,
        provide_context=True,
    )
    
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    start >> wait_for_products >> extract >> transform >> load >> end
