# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - MASTER ORCHESTRATOR DAG
# Orchestrates all ETL pipelines with proper sequencing and logging
# ═══════════════════════════════════════════════════════════════════════

"""
etl_master_orchestrator.py - Master Pipeline Orchestrator

PURPOSE:
- Single entry point to run entire ETL pipeline
- Triggers child DAGs in correct dependency order
- Waits for each stage to complete before proceeding
- Logs execution status and timing for all DAGs
- Provides centralized monitoring and control

EXECUTION ORDER:
  Stage 1 (Parallel): etl_customers, etl_products, etl_stores, etl_exchange_rates
  Stage 2 (Sequential): etl_sales (after products complete)
  Stage 3 (Final): etl_reports (after ALL tables complete)
    Stage 4 (Quality): etl_data_quality (after reports complete)

FEATURES:
- TriggerDagRunOperator: Triggers child DAGs
- ExternalTaskSensor: Waits for DAG completion
- Execution logging with timing metrics
- Configurable trigger modes (wait_for_completion)
"""

from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.task_group import TaskGroup

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from dag_base import (
    DEFAULT_ARGS, START_DATE, SCHEDULE_MIDNIGHT_IST,
    send_success_email, send_failure_email,
    DATA_PROCESSED,
    build_external_task_sensor
)


# ========================================
# CONFIGURATION
# ========================================

# Child DAGs to orchestrate
INGESTION_DAGS = ['etl_json_ingestion', 'etl_api_ingestion', 'etl_sql_ingestion']  # Phase 1 - Multi-format ingestion
INDEPENDENT_DAGS = ['etl_customers', 'etl_products', 'etl_stores', 'etl_exchange_rates']
DEPENDENT_DAGS = ['etl_sales']  # Depends on etl_products
FINAL_DAGS = ['etl_reports']    # Depends on ALL table DAGs
QUALITY_DAGS = ['etl_data_quality']  # Depends on reports and tables

# Sensor configuration - OPTIMIZED for 5-8 minute pipeline
SENSOR_TIMEOUT = 900  # 15 minutes timeout per DAG (down from 1 hour)
SENSOR_POKE_INTERVAL = 5  # Check every 5 seconds (down from 30s)


# ========================================
# HELPER FUNCTIONS
# ========================================

def log_stage_complete(**context):
    """Log the completion of a pipeline stage."""
    stage = context['params'].get('stage', 'Unknown')
    dags = context['params'].get('dags', [])
    ti = context['ti']
    
    # Get stage start time
    start_time_str = ti.xcom_pull(key=f'{stage}_start_time')
    if start_time_str:
        start_time = datetime.fromisoformat(start_time_str)
        duration = (datetime.now() - start_time).total_seconds()
    else:
        duration = 0
    
    print("=" * 60)
    print(f"✅ STAGE COMPLETE: {stage}")
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"✅ DAGs processed: {', '.join(dags)}")
    print("=" * 60)
    
    return {'stage': stage, 'dags': dags, 'duration_seconds': duration, 'status': 'completed'}


def get_triggered_run_id(context, dag_id):
    """Get the run_id of the DAG we just triggered."""
    # The triggered DAG run_id follows a pattern based on our trigger
    trigger_run_id = context['run_id']
    return f"triggered__{trigger_run_id}__{dag_id}"


def get_latest_execution_date(dt, external_dag_id):
    """Get the latest execution date for an external DAG (any state)."""
    dag_runs = DagRun.find(dag_id=external_dag_id)
    if dag_runs:
        # Get the most recent run
        latest_run = max(dag_runs, key=lambda x: x.execution_date)
        return latest_run.execution_date
    return dt


def log_stage_start(**context):
    """Log the start of a pipeline stage with detailed information."""
    stage = context['params'].get('stage', 'Unknown')
    dags = context['params'].get('dags', [])
    execution_date = context['execution_date']
    
    print("\n" + "=" * 80)
    print(f"🚀 STAGE START: {stage}")
    print("=" * 80)
    print(f"📅 Execution Date: {execution_date}")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 DAGs in this stage: {len(dags)}")
    print("-" * 80)
    
    # List each DAG that will be triggered
    for idx, dag_id in enumerate(dags, 1):
        # Check if DAG has run before
        recent_runs = DagRun.find(dag_id=dag_id, execution_date=execution_date)
        if recent_runs:
            latest_run = recent_runs[0]
            status = latest_run.state
            print(f"  {idx}. {dag_id:<30} | Previous run: {status}")
        else:
            print(f"  {idx}. {dag_id:<30} | Status: Ready to trigger")
    
    print("-" * 80)
    print(f"📊 Stage will trigger {len(dags)} DAG(s) in parallel")
    print("=" * 80 + "\n")
    
    context['ti'].xcom_push(key=f'{stage}_start_time', value=datetime.now().isoformat())
    return {'stage': stage, 'dags': dags, 'status': 'started'}


def log_stage_complete(**context):
    """Log the completion of a pipeline stage with detailed DAG results."""
    from sqlalchemy import create_engine, text
    import os
    
    stage = context['params'].get('stage', 'Unknown')
    dags = context['params'].get('dags', [])
    execution_date = context['execution_date']
    
    start_time_str = context['ti'].xcom_pull(key=f'{stage}_start_time')
    start_time = datetime.fromisoformat(start_time_str) if start_time_str else datetime.now()
    duration = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 80)
    print(f"✅ STAGE COMPLETE: {stage}")
    print("=" * 80)
    print(f"⏱️  Total Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
    print(f"⏰ End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    print(f"📊 DETAILED DAG RESULTS:")
    print("-" * 80)
    
    # Connect to database to get detailed run information
    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': 5432,
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
    }
    
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        
        success_count = 0
        failed_count = 0
        
        for idx, dag_id in enumerate(dags, 1):
            # Get latest run for this DAG
            dag_runs = DagRun.find(dag_id=dag_id)
            if dag_runs:
                latest_run = max(dag_runs, key=lambda x: x.execution_date)
                status = str(latest_run.state)
                status_icon = "✅" if status == "success" else "❌" if status == "failed" else "🔄"
                
                # Get task details
                task_instances = TaskInstance.find(
                    dag_id=dag_id,
                    run_id=latest_run.run_id,
                    state=[TaskInstanceState.SUCCESS, TaskInstanceState.FAILED]
                )
                
                total_tasks = len(task_instances)
                success_tasks = sum(1 for ti in task_instances if ti.state == TaskInstanceState.SUCCESS)
                failed_tasks = sum(1 for ti in task_instances if ti.state == TaskInstanceState.FAILED)
                
                # Get row counts from dag_run_summary if available
                try:
                    with engine.connect() as conn:
                        result = conn.execute(
                            text("""
                                SELECT rows_extracted, rows_loaded, rows_rejected 
                                FROM etl_output.dag_run_summary 
                                WHERE dag_id = :dag_id 
                                ORDER BY created_at DESC 
                                LIMIT 1
                            """),
                            {"dag_id": dag_id}
                        ).fetchone()
                        
                        if result:
                            rows_extracted, rows_loaded, rows_rejected = result
                            print(f"  {status_icon} {idx}. {dag_id:<30}")
                            print(f"       Status: {status:<15} | Tasks: {success_tasks}/{total_tasks} successful")
                            print(f"       Data: {rows_extracted:,} extracted → {rows_loaded:,} loaded | {rows_rejected:,} rejected")
                            print(f"       Duration: {(latest_run.end_date - latest_run.start_date).total_seconds():.2f}s")
                        else:
                            print(f"  {status_icon} {idx}. {dag_id:<30}")
                            print(f"       Status: {status:<15} | Tasks: {success_tasks}/{total_tasks} successful")
                            print(f"       Duration: {(latest_run.end_date - latest_run.start_date).total_seconds():.2f}s")
                except Exception as e:
                    print(f"  {status_icon} {idx}. {dag_id:<30}")
                    print(f"       Status: {status:<15} | Tasks: {success_tasks}/{total_tasks} successful")
                
                if status == "success":
                    success_count += 1
                elif status == "failed":
                    failed_count += 1
                    # Show failed task details
                    failed_task_names = [ti.task_id for ti in task_instances if ti.state == TaskInstanceState.FAILED]
                    if failed_task_names:
                        print(f"       ⚠️  Failed tasks: {', '.join(failed_task_names)}")
            else:
                print(f"  ⚪ {idx}. {dag_id:<30} | No run found")
        
        print("-" * 80)
        print(f"📈 STAGE SUMMARY:")
        print(f"   Total DAGs: {len(dags)} | Success: {success_count} | Failed: {failed_count}")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not retrieve detailed metrics: {str(e)}")
        print(f"📊 DAGs in stage: {', '.join(dags)}")
        print("=" * 80 + "\n")
    
    context['ti'].xcom_push(key=f'{stage}_duration', value=duration)
    return {'stage': stage, 'dags': dags, 'status': 'completed', 'duration': duration}


# ========================================
# Team 1 - T0031: Pipeline Execution Summary
# ========================================
def generate_execution_summary(**context):
    """Generate comprehensive execution summary for all DAGs."""
    ti = context['ti']
    run_id = context['run_id']
    execution_date = context['execution_date']
    
    print("\n" + "=" * 70)
    print("📊 MASTER ORCHESTRATOR - EXECUTION SUMMARY")
    print("=" * 70)
    
    # Collect timing for each stage
    stages = ['Stage1_Independent', 'Stage2_Sales', 'Stage3_Reports', 'Stage4_Quality']
    total_duration = 0
    stage_results = []
    
    for stage in stages:
        duration = ti.xcom_pull(key=f'{stage}_duration') or 0
        total_duration += duration
        stage_results.append({
            'stage': stage,
            'duration_seconds': duration,
            'duration_formatted': f"{duration:.2f}s"
        })
        print(f"  {stage}: {duration:.2f} seconds")
    
    print(f"\n⏱️  TOTAL PIPELINE DURATION: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    
    # Get status of all child DAGs
    all_dags = INGESTION_DAGS + INDEPENDENT_DAGS + DEPENDENT_DAGS + FINAL_DAGS + QUALITY_DAGS
    dag_statuses = []
    
    print("\n📋 CHILD DAG STATUS:")
    print("-" * 50)
    
    for dag_id in all_dags:
        dag_runs = DagRun.find(dag_id=dag_id, state=DagRunState.SUCCESS)
        if dag_runs:
            latest = max(dag_runs, key=lambda x: x.execution_date)
            status = 'SUCCESS'
            exec_date = latest.execution_date.isoformat()
        else:
            # Check for running or failed
            all_runs = DagRun.find(dag_id=dag_id)
            if all_runs:
                latest = max(all_runs, key=lambda x: x.execution_date)
                status = str(latest.state).upper()
                exec_date = latest.execution_date.isoformat()
            else:
                status = 'NO RUNS'
                exec_date = 'N/A'
        
        dag_statuses.append({
            'dag_id': dag_id,
            'status': status,
            'last_execution': exec_date
        })
        status_icon = '✅' if status == 'SUCCESS' else '❌' if status == 'FAILED' else '🔄'
        print(f"  {status_icon} {dag_id}: {status}")
    
    print("-" * 50)
    
    # Save summary to file
    summary = {
        'orchestrator_run_id': run_id,
        'execution_date': str(execution_date),
        'total_duration_seconds': total_duration,
        'stages': stage_results,
        'dag_statuses': dag_statuses,
        'generated_at': datetime.now().isoformat()
    }
    
    # Save to reports directory
    reports_dir = os.path.join(DATA_PROCESSED, 'reports')
    os.makedirs(reports_dir, exist_ok=True)
    
    summary_file = os.path.join(reports_dir, 'orchestrator_execution_summary.json')
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"\n💾 Summary saved to: {summary_file}")
    print("=" * 70)
    
    # Push summary to XCom
    ti.xcom_push(key='execution_summary', value=summary)
    
    return summary


# ========================================
# DAG DEFINITION
# ========================================

# ========================================
# Team 1 - T0023: Build Master DAG to Trigger All Pipelines
# ========================================
with DAG(
    dag_id='etl_master_orchestrator',
    default_args={
        **DEFAULT_ARGS,
        'on_failure_callback': send_failure_email,
    },
    description='Master Orchestrator - Triggers and monitors all ETL pipelines',
    start_date=START_DATE,
    schedule_interval=SCHEDULE_MIDNIGHT_IST,  # Daily at midnight IST (18:30 UTC) + manual trigger anytime
    catchup=False,  # Set to True if you want to backfill historical runs
    max_active_runs=1,  # Only one orchestrator run at a time
    tags=['team1', 'orchestrator', 'master'],
) as dag:
    
    # ========================================
    # PIPELINE START & END
    # ========================================
    
    start = EmptyOperator(task_id='pipeline_start')
    
    end = EmptyOperator(
        task_id='pipeline_end',
        on_success_callback=send_success_email,
    )
    
    # ========================================
    # INITIALIZATION TASK GROUP
    # ========================================
    
    with TaskGroup(
        group_id='initialization',
        tooltip='Pipeline initialization and logging setup'
    ) as init_group:
        
        log_pipeline_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Pipeline', 'dags': INGESTION_DAGS + INDEPENDENT_DAGS + DEPENDENT_DAGS + FINAL_DAGS},
        )
    
    # ========================================
    # STAGE 0: DATA INGESTION (PHASE 1)
    # ========================================
    
    with TaskGroup(
        group_id='stage0_data_ingestion',
        tooltip='Multi-format data ingestion: JSON, API, SQL (Phase 1 - Team 2)'
    ) as stage0_group:
        
        log_stage0_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage0_Ingestion', 'dags': INGESTION_DAGS},
        )
        
        log_stage0_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage0_Ingestion', 'dags': INGESTION_DAGS},
        )
        
        # JSON Ingestion sub-group
        with TaskGroup(
            group_id='json_ingestion',
            tooltip='Ingest JSON/JSONL files'
        ) as json_group:
            trigger_json = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_json_ingestion',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_json = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_json_ingestion',
                external_task_id='load_to_database',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_json.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_json_ingestion')
            trigger_json >> wait_json
        
        # API Ingestion sub-group
        with TaskGroup(
            group_id='api_ingestion',
            tooltip='Ingest data from REST APIs'
        ) as api_group:
            trigger_api = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_api_ingestion',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_api = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_api_ingestion',
                external_task_id='load_to_database',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_api.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_api_ingestion')
            trigger_api >> wait_api
        
        # SQL Ingestion sub-group
        with TaskGroup(
            group_id='sql_ingestion',
            tooltip='Ingest and transform data from SQL databases'
        ) as sql_group:
            trigger_sql = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_sql_ingestion',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_sql = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_sql_ingestion',
                external_task_id='load_to_database',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_sql.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_sql_ingestion')
            trigger_sql >> wait_sql
        
        # Stage 0 internal dependencies - All ingestion tasks run in parallel
        log_stage0_start >> [json_group, api_group, sql_group] >> log_stage0_complete
    
    # ========================================
    # STAGE 1: DIMENSION TABLES (PARALLEL)
    # ========================================
    
    with TaskGroup(
        group_id='stage1_dimension_tables',
        tooltip='Load dimension tables: Customers, Products, Stores, Exchange Rates'
    ) as stage1_group:
        
        log_stage1_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage1_Independent', 'dags': INDEPENDENT_DAGS},
        )
        
        log_stage1_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage1_Independent', 'dags': INDEPENDENT_DAGS},
        )
        
        # Customers sub-group
        with TaskGroup(
            group_id='customers',
            tooltip='ETL: Customers dimension table'
        ) as customers_group:
            trigger_customers = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_customers',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_customers = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_customers',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_customers.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_customers')
            trigger_customers >> wait_customers
        
        # Products sub-group
        with TaskGroup(
            group_id='products',
            tooltip='ETL: Products dimension table'
        ) as products_group:
            trigger_products = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_products',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_products = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_products',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_products.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_products')
            trigger_products >> wait_products
        
        # Stores sub-group
        with TaskGroup(
            group_id='stores',
            tooltip='ETL: Stores dimension table'
        ) as stores_group:
            trigger_stores = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_stores',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_stores = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_stores',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_stores.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_stores')
            trigger_stores >> wait_stores
        
        # Exchange Rates sub-group
        with TaskGroup(
            group_id='exchange_rates',
            tooltip='ETL: Exchange Rates dimension table'
        ) as exchange_rates_group:
            trigger_exchange_rates = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_exchange_rates',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_exchange_rates = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_exchange_rates',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_exchange_rates.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_exchange_rates')
            trigger_exchange_rates >> wait_exchange_rates
        
        # Stage 1 internal dependencies
        log_stage1_start >> [customers_group, products_group, stores_group, exchange_rates_group] >> log_stage1_complete
    
    # ========================================
    # STAGE 2: FACT TABLE (SALES)
    # ========================================
    
    with TaskGroup(
        group_id='stage2_fact_table',
        tooltip='Load fact table: Sales (depends on Products)'
    ) as stage2_group:
        
        log_stage2_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage2_Sales', 'dags': DEPENDENT_DAGS},
        )
        
        with TaskGroup(
            group_id='sales',
            tooltip='ETL: Sales fact table'
        ) as sales_group:
            trigger_sales = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_sales',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_sales = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_sales',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_sales.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_sales')
            trigger_sales >> wait_sales
        
        log_stage2_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage2_Sales', 'dags': DEPENDENT_DAGS},
        )
        
        log_stage2_start >> sales_group >> log_stage2_complete
    
    # ========================================
    # STAGE 3: ANALYTICS & REPORTS
    # ========================================
    
    with TaskGroup(
        group_id='stage3_reports',
        tooltip='Generate analytics reports (depends on ALL tables)'
    ) as stage3_group:
        
        log_stage3_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage3_Reports', 'dags': FINAL_DAGS},
        )
        
        with TaskGroup(
            group_id='reports',
            tooltip='Generate all analytics reports'
        ) as reports_group:
            trigger_reports = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_reports',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_reports = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_reports',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_reports.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_reports')
            trigger_reports >> wait_reports
        
        log_stage3_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage3_Reports', 'dags': FINAL_DAGS},
        )
        
        log_stage3_start >> reports_group >> log_stage3_complete

    # ========================================
    # STAGE 4: DATA QUALITY
    # ========================================

    with TaskGroup(
        group_id='stage4_quality',
        tooltip='Run data quality pipeline (depends on reports)'
    ) as stage4_group:

        log_stage4_start = PythonOperator(
            task_id='log_start',
            python_callable=log_stage_start,
            params={'stage': 'Stage4_Quality', 'dags': QUALITY_DAGS},
        )

        with TaskGroup(
            group_id='data_quality',
            tooltip='ETL: Data quality checks and scorecards'
        ) as quality_group:
            trigger_quality = TriggerDagRunOperator(
                task_id='trigger',
                trigger_dag_id='etl_data_quality',
                wait_for_completion=False,
                reset_dag_run=False,
                poke_interval=5,
            )
            wait_quality = build_external_task_sensor(
                task_id='wait_complete',
                external_dag_id='etl_data_quality',
                external_task_id='end',
                timeout=SENSOR_TIMEOUT,
                poke_interval=SENSOR_POKE_INTERVAL,
            )
            wait_quality.execution_date_fn = lambda dt: get_latest_execution_date(dt, 'etl_data_quality')
            trigger_quality >> wait_quality

        log_stage4_complete = PythonOperator(
            task_id='log_complete',
            python_callable=log_stage_complete,
            params={'stage': 'Stage4_Quality', 'dags': QUALITY_DAGS},
        )

        log_stage4_start >> quality_group >> log_stage4_complete
    
    # ========================================
    # FINALIZATION TASK GROUP
    # ========================================
    
    with TaskGroup(
        group_id='finalization',
        tooltip='Generate execution summary and finalize pipeline'
    ) as final_group:
        
        generate_summary = PythonOperator(
            task_id='generate_summary',
            python_callable=generate_execution_summary,
        )
    
    # ========================================
    # MAIN PIPELINE DEPENDENCY CHAIN
    # ========================================
    
    # Pipeline flow: Start → Init → Stage0 (Ingestion) → Stage1 (Dimensions) → Stage2 (Sales) → Stage3 (Reports) → Stage4 (Quality) → Final → End
    start >> init_group >> stage0_group >> stage1_group >> stage2_group >> stage3_group >> stage4_group >> final_group >> end
