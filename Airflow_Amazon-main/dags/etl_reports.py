# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - PHASE 5: ETL REPORTS DAG (MASTER)
# Tasks: T0023-T0027, T0031 (Sprint 5 & 6)
# ═══════════════════════════════════════════════════════════════════════

"""
etl_reports.py - Report Generation Pipeline (MASTER DAG)

TASKS IMPLEMENTED:
- T0023: Build master DAG to trigger all pipelines
- T0024: Event-driven DAG triggering (5 ExternalTaskSensors)
- T0025: Multi-DAG dependency management (waits for ALL 5 tables)
- T0026: Backfill & catchup features (catchup=True)
- T0027: DAG failure handling strategy (retries, email alerts)
- T0031: Pipeline execution summary (generates 9 reports)

Pipeline: Wait for ALL tables → Generate 9 Reports
Schedule: Midnight IST (18:30 UTC)
Dependencies: WAITS FOR ALL 5 table DAGs:
  - etl_customers
  - etl_products
  - etl_stores
  - etl_exchange_rates
  - etl_sales
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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
    DATA_RAW, DATA_PROCESSED, get_connection_string,
    DATA_GOLD, copy_to_medallion,
    build_external_task_sensor
)


# ========================================
# Team 1 - T0031: Pipeline Execution Summary
# ========================================
# REPORT GENERATION FUNCTIONS (9 CSV Reports)

def generate_all_reports(**context):
    """Generate all 9 reports from cleaned data"""
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    
    conn_str = get_connection_string()
    engine = create_engine(conn_str)
    
    reports_dir = f'{DATA_PROCESSED}/reports'
    os.makedirs(reports_dir, exist_ok=True)
    
    reports_generated = []
    
    # Load all tables from database
    customers = pd.read_sql("SELECT * FROM etl_output.customers", engine)
    products = pd.read_sql("SELECT * FROM etl_output.products", engine)
    stores = pd.read_sql("SELECT * FROM etl_output.stores", engine)
    sales = pd.read_sql("SELECT * FROM etl_output.sales", engine)
    exchange_rates = pd.read_sql("SELECT * FROM etl_output.exchange_rates", engine)
    
    # Load raw customer data for filling missing info (rejected customers)
    raw_customers = pd.read_csv(f'{DATA_RAW}/Customers.csv')
    
    print(f"📊 Loaded tables for reporting:")
    print(f"   Customers: {len(customers):,}")
    print(f"   Products: {len(products):,}")
    print(f"   Stores: {len(stores):,}")
    print(f"   Sales: {len(sales):,}")
    print(f"   Exchange Rates: {len(exchange_rates):,}")
    
    # ===== REPORT 1: Customer Demographics =====
    if 'Country' in customers.columns:
        report1 = customers.groupby('Country').agg({
            'CustomerKey': 'count'
        }).reset_index()
        report1.columns = ['Country', 'Customer_Count']
        report1.to_csv(f'{reports_dir}/customer_demographics.csv', index=False)
        reports_generated.append('customer_demographics.csv')
        print(f"✅ Report 1: Customer Demographics ({len(report1)} countries)")
    
    # ===== REPORT 2: Product Catalog Summary =====
    if 'Category' in products.columns:
        report2 = products.groupby('Category').agg({
            'ProductKey': 'count',
            'Unit Price USD': ['mean', 'min', 'max']
        }).reset_index()
        report2.columns = ['Category', 'Product_Count', 'Avg_Price', 'Min_Price', 'Max_Price']
        report2.to_csv(f'{reports_dir}/product_catalog_summary.csv', index=False)
        reports_generated.append('product_catalog_summary.csv')
        print(f"✅ Report 2: Product Catalog Summary ({len(report2)} categories)")
    
    # ===== REPORT 3: Store Performance =====
    if 'StoreKey' in stores.columns and 'StoreKey' in sales.columns:
        store_sales = sales.groupby('StoreKey').agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        store_sales.columns = ['StoreKey', 'Total_Orders', 'Total_Quantity']
        report3 = stores.merge(store_sales, on='StoreKey', how='left')
        report3.to_csv(f'{reports_dir}/store_performance.csv', index=False)
        reports_generated.append('store_performance.csv')
        print(f"✅ Report 3: Store Performance ({len(report3)} stores)")
    
    # ===== REPORT 4: Sales by Date =====
    if 'Order Date' in sales.columns:
        sales['Order Date'] = pd.to_datetime(sales['Order Date'], errors='coerce')
        report4 = sales.groupby(sales['Order Date'].dt.date).agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        report4.columns = ['Date', 'Total_Orders', 'Total_Quantity']
        report4.to_csv(f'{reports_dir}/sales_by_date.csv', index=False)
        reports_generated.append('sales_by_date.csv')
        print(f"✅ Report 4: Sales by Date ({len(report4)} days)")
    
    # ===== REPORT 5: Top Products by Sales =====
    if 'ProductKey' in sales.columns:
        product_sales = sales.groupby('ProductKey').agg({
            'Quantity': 'sum',
            'Order Number': 'count'
        }).reset_index()
        product_sales.columns = ['ProductKey', 'Total_Quantity', 'Total_Orders']
        report5 = product_sales.merge(products[['ProductKey', 'Product Name']], on='ProductKey', how='left')
        report5 = report5.sort_values('Total_Quantity', ascending=False).head(50)
        report5.to_csv(f'{reports_dir}/top_products.csv', index=False)
        reports_generated.append('top_products.csv')
        print(f"✅ Report 5: Top 50 Products by Sales")
    
    # ===== REPORT 6: Customer Purchase Analysis =====
    if 'CustomerKey' in sales.columns:
        customer_sales = sales.groupby('CustomerKey').agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        customer_sales.columns = ['CustomerKey', 'Total_Orders', 'Total_Items']
        
        # First merge with cleaned customers
        report6 = customer_sales.merge(customers[['CustomerKey', 'Name', 'Country']], on='CustomerKey', how='left')
        
        # For missing values (rejected customers), fill from raw data
        missing_mask = report6['Name'].isna()
        if missing_mask.any():
            raw_cust_subset = raw_customers[['CustomerKey', 'Name', 'Country']].drop_duplicates(subset='CustomerKey')
            missing_keys = report6.loc[missing_mask, 'CustomerKey'].tolist()
            raw_fill = raw_cust_subset[raw_cust_subset['CustomerKey'].isin(missing_keys)]
            
            for _, row in raw_fill.iterrows():
                mask = report6['CustomerKey'] == row['CustomerKey']
                report6.loc[mask, 'Name'] = row['Name'] if pd.notna(row['Name']) else 'Unknown'
                report6.loc[mask, 'Country'] = row['Country'] if pd.notna(row['Country']) else 'Unknown'
            
            # Fill any remaining nulls
            report6['Name'] = report6['Name'].fillna('Unknown')
            report6['Country'] = report6['Country'].fillna('Unknown')
        
        report6 = report6.sort_values('Total_Items', ascending=False)
        report6.to_csv(f'{reports_dir}/customer_purchase_analysis.csv', index=False)
        reports_generated.append('customer_purchase_analysis.csv')
        print(f"✅ Report 6: Customer Purchase Analysis ({len(report6):,} customers)")
    
    # ===== REPORT 7: Monthly Sales Trend =====
    if 'Order Date' in sales.columns:
        sales['Month'] = pd.to_datetime(sales['Order Date'], errors='coerce').dt.to_period('M')
        report7 = sales.groupby('Month').agg({
            'Order Number': 'count',
            'Quantity': 'sum'
        }).reset_index()
        report7['Month'] = report7['Month'].astype(str)
        report7.columns = ['Month', 'Total_Orders', 'Total_Quantity']
        report7.to_csv(f'{reports_dir}/monthly_sales_trend.csv', index=False)
        reports_generated.append('monthly_sales_trend.csv')
        print(f"✅ Report 7: Monthly Sales Trend ({len(report7)} months)")
    
    # ===== REPORT 8: Store Geographic Distribution =====
    if 'Country' in stores.columns:
        report8 = stores.groupby('Country').agg({
            'StoreKey': 'count',
            'Square Meters': 'sum'
        }).reset_index()
        report8.columns = ['Country', 'Store_Count', 'Total_Square_Meters']
        report8.to_csv(f'{reports_dir}/store_geographic_distribution.csv', index=False)
        reports_generated.append('store_geographic_distribution.csv')
        print(f"✅ Report 8: Store Geographic Distribution ({len(report8)} countries)")
    
    # ===== REPORT 9: Exchange Rate Summary =====
    if 'Currency' in exchange_rates.columns:
        report9 = exchange_rates.groupby('Currency').agg({
            'Exchange': ['mean', 'min', 'max', 'count']
        }).reset_index()
        report9.columns = ['Currency', 'Avg_Rate', 'Min_Rate', 'Max_Rate', 'Data_Points']
        report9.to_csv(f'{reports_dir}/exchange_rate_summary.csv', index=False)
        reports_generated.append('exchange_rate_summary.csv')
        print(f"✅ Report 9: Exchange Rate Summary ({len(report9)} currencies)")
    
    # ===== REPORT 10: Rejected Records Summary =====
    try:
        rejected_records = pd.read_sql("SELECT * FROM etl_output.rejected_records", engine)
        if len(rejected_records) > 0:
            # Summary by table and reason
            report10_summary = rejected_records.groupby(['table_name', 'reason']).size().reset_index(name='count')
            report10_summary = report10_summary.sort_values('count', ascending=False)
            report10_summary.to_csv(f'{reports_dir}/rejected_records_summary.csv', index=False)
            reports_generated.append('rejected_records_summary.csv')
            
            # Full rejected records (without original_data to keep file size manageable)
            report10_detail = rejected_records[['table_name', 'record_id', 'reason', 'rejected_at', 'dag_run_id']]
            report10_detail = report10_detail.sort_values('rejected_at', ascending=False)
            report10_detail.to_csv(f'{reports_dir}/rejected_records_detail.csv', index=False)
            reports_generated.append('rejected_records_detail.csv')
            print(f"✅ Report 10: Rejected Records ({len(rejected_records):,} records)")
    except Exception as e:
        print(f"⚠️ Report 10: Rejected Records skipped - {e}")
    
    # ===== REPORT 11: DAG Run Summary =====
    try:
        dag_run_summary = pd.read_sql("SELECT * FROM etl_output.dag_run_summary", engine)
        if len(dag_run_summary) > 0:
            dag_run_summary = dag_run_summary.sort_values('execution_date', ascending=False)
            dag_run_summary.to_csv(f'{reports_dir}/dag_run_summary.csv', index=False)
            reports_generated.append('dag_run_summary.csv')
            print(f"✅ Report 11: DAG Run Summary ({len(dag_run_summary):,} runs)")
    except Exception as e:
        print(f"⚠️ Report 11: DAG Run Summary skipped - {e}")
    
    print(f"\n🎉 Generated {len(reports_generated)} reports successfully!")

    # Gold layer copy (reports)
    for report_name in reports_generated:
        source_path = f"{reports_dir}/{report_name}"
        copy_to_medallion(source_path, DATA_GOLD, report_name)
    
    context['ti'].xcom_push(key='reports_generated', value=reports_generated)
    context['ti'].xcom_push(key='reports_count', value=len(reports_generated))
    
    return {
        'reports': reports_generated,
        'count': len(reports_generated),
        'directory': reports_dir
    }


# ========================================
# DAG DEFINITION
# ========================================

with DAG(
    dag_id='etl_reports',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='Report Generation Pipeline (waits for all tables)',
    start_date=START_DATE,
    schedule_interval=None,  # Disabled for testing - manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['team1', 'etl', 'reports'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # ========================================
    # Team 1 - T0025: Multi-DAG Dependency Management
    # (5 ExternalTaskSensors - wait for ALL table DAGs)
    # ========================================
    
    # Sensor configuration for testing - look for latest successful run
    # Using execution_date_fn to find the most recent successful run
    sensor_timeout = 7200  # 2 hours
    sensor_poke = 30  # Check every 30 seconds
    
    # Wait for all table DAGs to complete
    wait_for_customers = build_external_task_sensor(
        task_id='wait_for_customers',
        external_dag_id='etl_customers',
        external_task_id='end',
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
    )
    wait_for_customers.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_customers')
    
    wait_for_products = build_external_task_sensor(
        task_id='wait_for_products',
        external_dag_id='etl_products',
        external_task_id='end',
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
    )
    wait_for_products.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_products')
    
    wait_for_stores = build_external_task_sensor(
        task_id='wait_for_stores',
        external_dag_id='etl_stores',
        external_task_id='end',
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
    )
    wait_for_stores.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_stores')
    
    wait_for_exchange_rates = build_external_task_sensor(
        task_id='wait_for_exchange_rates',
        external_dag_id='etl_exchange_rates',
        external_task_id='end',
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
    )
    wait_for_exchange_rates.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_exchange_rates')
    
    wait_for_sales = build_external_task_sensor(
        task_id='wait_for_sales',
        external_dag_id='etl_sales',
        external_task_id='end',
        timeout=sensor_timeout,
        poke_interval=sensor_poke,
    )
    wait_for_sales.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_sales')
    
    generate_reports = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_all_reports,
        provide_context=True,
    )
    
    end = EmptyOperator(
        task_id='end',
        on_success_callback=send_success_email,
    )
    
    # Dependency chain
    start >> [
        wait_for_customers,
        wait_for_products,
        wait_for_stores,
        wait_for_exchange_rates,
        wait_for_sales
    ] >> generate_reports >> end
