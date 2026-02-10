# ═══════════════════════════════════════════════════════════════════════
# PHASE 1 - TEAM 2 SPRINT 2: Data Quality & Validation DAG
# Tasks: T0008, T0009, T0010, T0011, T0012
# ═══════════════════════════════════════════════════════════════════════

"""
etl_data_quality.py - Comprehensive Data Quality Pipeline

TASKS IMPLEMENTED:
- T0008: Column-level validation (regex, min/max, allowed values)
- T0009: Data profiling (statistical analysis)
- T0010: Quality rules execution (completeness, accuracy, consistency)
- T0011: Anomaly detection (Z-score outliers)
- T0012: Quality scorecards & dashboards

Pipeline Flow:
1. Load cleaned data from database
2. Run column-level validations
3. Generate data profiles
4. Execute quality rules
5. Detect anomalies
6. Generate quality scorecard
7. Store results in database

Schedule: Daily at 5 AM (after all ETL DAGs complete)
Dependencies: Runs after dimension and fact tables are loaded
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import yaml
import json
sys.path.insert(0, '/opt/airflow/scripts')

from utils.validation_utils import DataValidator
from Load import DatabaseLoader
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════

TABLES = ['customers', 'products', 'stores', 'exchange_rates', 'sales']

# Load validation rules
try:
    with open('/opt/airflow/config/validation_config.yaml', 'r') as f:
        CONFIG = yaml.safe_load(f)
except:
    logger.warning("Could not load validation_config.yaml, using defaults")
    CONFIG = {'validation_rules': {}, 'quality_rules': {}, 'anomaly_detection': {}}

# ═══════════════════════════════════════════════════════════════════════
# Task 1: Column-Level Validation (T0008)
# ═══════════════════════════════════════════════════════════════════════

def validate_table_columns(**context):
    """Run column-level validations for all tables"""
    logger.info("🔍 Starting Column-Level Validation")
    
    db_loader = DatabaseLoader()
    db_loader.connect()
    all_results = []
    
    for table in TABLES:
        logger.info(f"\\n▶ Validating table: {table}")
        
        # Load data
        try:
            df = pd.read_sql_table(table, db_loader.engine, schema='etl_output')
        except Exception as e:
            logger.error(f"Failed to load {table}: {e}")
            continue
        
        # Get validation rules for this table
        table_rules = CONFIG.get('validation_rules', {}).get(table, {})
        
        if not table_rules:
            logger.warning(f"No validation rules defined for {table}")
            continue
        
        # Validate each column
        for column, rules in table_rules.items():
            if column not in df.columns:
                continue
            
            is_valid, details = DataValidator.validate_column_rules(df, column, rules)
            
            result = {
                'table_name': table,
                'column_name': column,
                'is_valid': is_valid,
                'total_rows': details['total_rows'],
                'passed': details['passed'],
                'failed': details['failed'],
                'failures': ', '.join(details.get('failures', [])),
                'validated_at': datetime.now()
            }
            all_results.append(result)
            
            status = "✅ PASS" if is_valid else "❌ FAIL"
            logger.info(f"  {column}: {status} ({details['passed']}/{details['total_rows']} passed)")
    
    # Store results in database
    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df.to_sql('validation_results', db_loader.engine, 
                         schema='etl_output', if_exists='replace', index=False)
        
        failed_count = sum(1 for r in all_results if not r['is_valid'])
        logger.info(f"\\n✅ Validation Complete: {len(all_results)} checks, {failed_count} failures")
    
    context['task_instance'].xcom_push(key='validation_results', value=all_results)

# ═══════════════════════════════════════════════════════════════════════
# Task 2: Data Profiling (T0009)
# ═══════════════════════════════════════════════════════════════════════

def profile_all_tables(**context):
    """Generate statistical profiles for all tables"""
    logger.info("📊 Starting Data Profiling")
    
    db_loader = DatabaseLoader()
    db_loader.connect()
    all_profiles = []
    
    for table in TABLES:
        logger.info(f"\\n▶ Profiling table: {table}")
        
        try:
            df = pd.read_sql_table(table, db_loader.engine, schema='etl_output')
            profile = DataValidator.profile_dataframe(df, table_name=table)
            all_profiles.append(profile)
            
            logger.info(f"  Rows: {profile['row_count']:,}, Columns: {profile['column_count']}")
            logger.info(f"  Memory: {profile['memory_usage_mb']:.2f} MB")
            
            # Log key metrics
            for col, col_profile in profile['columns'].items():
                null_pct = col_profile['null_percent']
                unique_count = col_profile['unique_count']
                logger.info(f"    {col}: {null_pct}% null, {unique_count} unique values")
        
        except Exception as e:
            logger.error(f"Failed to profile {table}: {e}")
    
    # Store profiles in database as JSON
    if all_profiles:
        profiles_df = pd.DataFrame([{
            'table_name': p['table_name'],
            'row_count': p['row_count'],
            'column_count': p['column_count'],
            'memory_usage_mb': p['memory_usage_mb'],
            'profile_json': json.dumps(p),
            'generated_at': datetime.now()
        } for p in all_profiles])
        
        profiles_df.to_sql('data_profiles', db_loader.engine,
                          schema='etl_output', if_exists='replace', index=False)
        
        logger.info(f"\\n✅ Profiling Complete: {len(all_profiles)} tables profiled")
    
    context['task_instance'].xcom_push(key='profiles', value=all_profiles)

# ═══════════════════════════════════════════════════════════════════════
# Task 3: Quality Rules Execution (T0010)
# ═══════════════════════════════════════════════════════════════════════

def execute_quality_rules(**context):
    """Execute quality rules for all tables"""
    logger.info("🎯 Executing Quality Rules")
    
    db_loader = DatabaseLoader()
    db_loader.connect()
    all_checks = []
    
    for table in TABLES:
        logger.info(f"\\n▶ Quality checks for: {table}")
        
        try:
            df = pd.read_sql_table(table, db_loader.engine, schema='etl_output')
        except Exception as e:
            logger.error(f"Failed to load {table}: {e}")
            continue
        
        # Get quality rules
        table_quality_rules = CONFIG.get('quality_rules', {}).get(table, {})
        
        # Check completeness rules
        for rule in table_quality_rules.get('completeness', []):
            column = rule['column']
            threshold = rule['threshold']
            
            if column not in df.columns:
                logger.warning(f"  Completeness {column}: ⚠️ column not found, skipping")
                continue
            
            null_count = df[column].isnull().sum()
            completeness = ((len(df) - null_count) / len(df)) * 100
            
            passed = completeness >= threshold
            
            all_checks.append({
                'table_name': table,
                'rule_type': 'completeness',
                'column_name': column,
                'threshold': threshold,
                'actual_value': round(completeness, 2),
                'passed': passed,
                'severity': rule.get('severity', 'medium'),
                'checked_at': datetime.now()
            })
            
            status = "✅ PASS" if passed else "❌ FAIL"
            logger.info(f"  Completeness {column}: {status} ({completeness:.2f}% >= {threshold}%)") 
        
        # Check accuracy rules (range checks)
        for rule in table_quality_rules.get('accuracy', []):
            if rule.get('rule_type') == 'range':
                column = rule['column']
                min_val = rule.get('min_value')
                max_val = rule.get('max_value')
                threshold = rule['threshold']
                
                if column not in df.columns:
                    logger.warning(f"  Range Check {column}: ⚠️ column not found, skipping")
                    continue
                
                numeric_col = pd.to_numeric(df[column], errors='coerce')
                in_range = numeric_col.between(min_val, max_val).sum()
                accuracy = (in_range / len(df)) * 100
                
                passed = accuracy >= threshold
                
                all_checks.append({
                    'table_name': table,
                    'rule_type': 'accuracy_range',
                    'column_name': column,
                    'threshold': threshold,
                    'actual_value': round(accuracy, 2),
                    'passed': passed,
                    'severity': rule.get('severity', 'medium'),
                    'checked_at': datetime.now()
                })
                
                status = "✅ PASS" if passed else "❌ FAIL"
                logger.info(f"  Range Check {column}: {status} ({accuracy:.2f}% in range)")
    
    # Store quality check results
    if all_checks:
        checks_df = pd.DataFrame(all_checks)
        checks_df.to_sql('quality_check_results', db_loader.engine,
                        schema='etl_output', if_exists='replace', index=False)
        
        failed = sum(1 for c in all_checks if not c['passed'])
        logger.info(f"\\n✅ Quality Checks Complete: {len(all_checks)} checks, {failed} failures")
    
    context['task_instance'].xcom_push(key='quality_checks', value=all_checks)

# ═══════════════════════════════════════════════════════════════════════
# Task 4: Anomaly Detection (T0011)
# ═══════════════════════════════════════════════════════════════════════

def detect_anomalies(**context):
    """Detect anomalies using Z-score method"""
    logger.info("🚨 Starting Anomaly Detection")
    
    db_loader = DatabaseLoader()
    db_loader.connect()
    all_anomalies = []
    
    anomaly_config = CONFIG.get('anomaly_detection', {})
    
    for table in TABLES:
        if table not in anomaly_config:
            continue
        
        logger.info(f"\\n▶ Detecting anomalies in: {table}")
        
        try:
            df = pd.read_sql_table(table, db_loader.engine, schema='etl_output')
        except Exception as e:
            logger.error(f"Failed to load {table}: {e}")
            continue
        
        # Check each column configured for anomaly detection
        for column, config in anomaly_config[table].items():
            if column not in df.columns:
                continue
            
            threshold = config.get('threshold', 3.0)
            method = config.get('method', 'zscore')
            
            if method == 'zscore':
                anomalies_df, summary = DataValidator.detect_anomalies_zscore(
                    df, column, threshold
                )
                
                if len(anomalies_df) > 0:
                    # Store anomalies
                    anomalies_df['table_name'] = table
                    anomalies_df['column_name'] = column
                    anomalies_df['detection_method'] = method
                    anomalies_df['detected_at'] = datetime.now()
                    
                    all_anomalies.append(anomalies_df)
                    
                    logger.info(f"  {column}: Found {summary['anomaly_count']} anomalies "
                              f"({summary['anomaly_percent']}%)")
                else:
                    logger.info(f"  {column}: No anomalies detected ✅")
    
    # Store all anomalies
    if all_anomalies:
        combined_anomalies = pd.concat(all_anomalies, ignore_index=True)
        combined_anomalies.to_sql('detected_anomalies', db_loader.engine,
                                 schema='etl_output', if_exists='replace', index=False)
        
        logger.info(f"\\n✅ Anomaly Detection Complete: {len(combined_anomalies)} anomalies found")
    else:
        logger.info("\\n✅ No anomalies detected across all tables")
    
    context['task_instance'].xcom_push(key='anomalies_count', 
                                       value=len(combined_anomalies) if all_anomalies else 0)

# ═══════════════════════════════════════════════════════════════════════
# Task 5: Generate Quality Scorecard (T0012)
# ═══════════════════════════════════════════════════════════════════════

def generate_quality_scorecard(**context):
    """Generate comprehensive quality scorecard"""
    logger.info("📈 Generating Quality Scorecard")
    
    db_loader = DatabaseLoader()
    db_loader.connect()
    
    # Get results from previous tasks
    quality_checks = context['task_instance'].xcom_pull(
        task_ids='execute_quality_rules', key='quality_checks'
    ) or []
    
    anomalies_count = context['task_instance'].xcom_pull(
        task_ids='detect_anomalies', key='anomalies_count'
    ) or 0
    
    # Calculate scores per table
    scorecard_data = []
    
    for table in TABLES:
        try:
            df = pd.read_sql_table(table, db_loader.engine, schema='etl_output')
            
            # Get checks for this table
            table_checks = [c for c in quality_checks if c['table_name'] == table]
            
            if table_checks:
                passed = sum(1 for c in table_checks if c['passed'])
                total = len(table_checks)
                quality_score = (passed / total) * 100
            else:
                quality_score = 100  # No checks = assume good
            
            # Calculate dimension scores
            completeness_checks = [c for c in table_checks if c['rule_type'] == 'completeness']
            accuracy_checks = [c for c in table_checks if 'accuracy' in c['rule_type']]
            
            completeness_score = (sum(1 for c in completeness_checks if c['passed']) / len(completeness_checks) * 100) if completeness_checks else 100
            accuracy_score = (sum(1 for c in accuracy_checks if c['passed']) / len(accuracy_checks) * 100) if accuracy_checks else 100
            
            scorecard_data.append({
                'table_name': table,
                'row_count': len(df),
                'overall_score': round(quality_score, 2),
                'completeness_score': round(completeness_score, 2),
                'accuracy_score': round(accuracy_score, 2),
                'total_checks': len(table_checks),
                'passed_checks': sum(1 for c in table_checks if c['passed']),
                'failed_checks': sum(1 for c in table_checks if not c['passed']),
                'status': 'PASS' if quality_score >= 80 else 'FAIL',
                'generated_at': datetime.now()
            })
            
            logger.info(f"  {table}: {quality_score:.1f}% quality score")
        
        except Exception as e:
            logger.error(f"Failed to score {table}: {e}")
    
    # Store scorecard
    if scorecard_data:
        scorecard_df = pd.DataFrame(scorecard_data)
        scorecard_df.to_sql('quality_scorecards', db_loader.engine,
                           schema='etl_output', if_exists='replace', index=False)
        
        avg_score = scorecard_df['overall_score'].mean()
        logger.info(f"\\n✅ Quality Scorecard Generated")
        logger.info(f"   Average Quality Score: {avg_score:.1f}%")
        logger.info(f"   Total Anomalies: {anomalies_count}")

# ═══════════════════════════════════════════════════════════════════════
# DAG Definition
# ═══════════════════════════════════════════════════════════════════════

default_args = {
    'owner': 'team2',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_data_quality',
    default_args=default_args,
    description='Phase 1 - Data Quality & Validation Pipeline (T0008-T0012)',
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    start_date=datetime(2026, 1, 22),
    catchup=False,
    tags=['team2', 'phase1', 'quality', 'validation'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # T0008: Column-Level Validation
    validate_columns = PythonOperator(
        task_id='validate_columns',
        python_callable=validate_table_columns,
        provide_context=True,
    )
    
    # T0009: Data Profiling
    profile_data = PythonOperator(
        task_id='profile_data',
        python_callable=profile_all_tables,
        provide_context=True,
    )
    
    # T0010: Quality Rules
    check_quality = PythonOperator(
        task_id='execute_quality_rules',
        python_callable=execute_quality_rules,
        provide_context=True,
    )
    
    # T0011: Anomaly Detection
    find_anomalies = PythonOperator(
        task_id='detect_anomalies',
        python_callable=detect_anomalies,
        provide_context=True,
    )
    
    # T0012: Quality Scorecard
    create_scorecard = PythonOperator(
        task_id='generate_scorecard',
        python_callable=generate_quality_scorecard,
        provide_context=True,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Pipeline flow
    start >> validate_columns >> profile_data >> check_quality >> find_anomalies >> create_scorecard >> end
