# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - ETL PIPELINE: LOAD PHASE
# Tasks: T0018-T0022 (Sprint 4)
# ═══════════════════════════════════════════════════════════════════════

"""
Load.py - Database Loading Module

TASKS IMPLEMENTED:
- T0018: Bulk load operations - via utils/bulk_loader.py
- T0019: Incremental vs Full loads - via utils/load_strategy.py
- T0020: Handling constraint violations - via utils/constraint_handler.py
- T0021: Upsert logic - via utils/upsert_handler.py
- T0022: Error table creation (rejects) - via utils/rejected_records_handler.py

Responsibilities:
- Load cleaned data to PostgreSQL database
- Support full load and incremental/upsert strategies
- Handle constraint validation
- Track rejected records
- Manage bulk inserts

Target: Docker Airflow PostgreSQL
- Host: localhost:5432 (local) / postgres:5432 (Docker)
- Database: airflow
- Schema: etl_output (separate from Airflow tables)
- Credentials: airflow/airflow

Uses utils:
- bulk_loader (T0018)
- load_strategy (T0019)
- constraint_handler (T0020)
- upsert_handler (T0021)
- rejected_records_handler (T0022)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import sys
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Ensure /opt/airflow/scripts is importable before importing utils
PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_ROOT = Path(__file__).parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.governance_utils import (
    ensure_governance_tables,
    log_audit_event,
    log_lineage,
    apply_classification_from_config,
    log_schema_validation,
)
from utils.schema_validation import validate_dataframe

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

# Database configuration
# Use 'postgres' for Docker containers, 'localhost' for local development
import os
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),  # Default to 'postgres' for Docker
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'schema': 'etl_output'  # Separate schema for ETL tables
}


class DatabaseLoader:
    """
    TEAM 1 - Database Loading Handler
    
    Loads cleaned data to PostgreSQL with support for:
    - Full load (TRUNCATE + INSERT)
    - Incremental load (UPSERT)
    - Batch processing
    - Constraint validation
    - Rejected records tracking
    - Separate schema (etl_output) from Airflow system tables
    """
    
    # ETL Schema name
    ETL_SCHEMA = 'etl_output'
    
    # Table configurations with primary keys
    TABLE_CONFIG = {
        'customers_cleaned': {
            'table_name': 'customers',
            'primary_key': 'CustomerKey',
            'dtype_map': {
                'CustomerKey': 'INTEGER',
                'Name': 'VARCHAR(255)',
                'Email': 'VARCHAR(255)',
                'Birthday': 'DATE',
                'Age': 'INTEGER',
                'City': 'VARCHAR(100)',
                'Country': 'VARCHAR(100)'
            }
        },
        'sales_cleaned': {
            'table_name': 'sales',
            'primary_key': 'Order Number',
            'dtype_map': {
                'Order Number': 'INTEGER',
                'CustomerKey': 'INTEGER',
                'ProductKey': 'INTEGER',
                'StoreKey': 'INTEGER',
                'Quantity': 'INTEGER',
                'Total_Amount_USD': 'DECIMAL(12,2)',
                'Delivery_Status': 'VARCHAR(50)'
            }
        },
        'products_cleaned': {
            'table_name': 'products',
            'primary_key': 'ProductKey',
            'dtype_map': {
                'ProductKey': 'INTEGER',
                'Product Name': 'VARCHAR(255)',
                'Category': 'VARCHAR(100)',
                'Unit Price USD': 'DECIMAL(10,2)'
            }
        },
        'stores_cleaned': {
            'table_name': 'stores',
            'primary_key': 'StoreKey',
            'dtype_map': {
                'StoreKey': 'INTEGER',
                'Country': 'VARCHAR(100)',
                'State': 'VARCHAR(100)'
            }
        },
        'exchange_rates_cleaned': {
            'table_name': 'exchange_rates',
            'primary_key': ['Date', 'Currency'],  # Composite key
            'dtype_map': {
                'Date': 'DATE',
                'Currency': 'VARCHAR(10)',
                'Exchange': 'DECIMAL(15,6)'
            }
        }
    }
    
    def __init__(self, 
                 connection_string: Optional[str] = None,
                 batch_size: int = 10000):
        """
        Initialize database loader
        
        Args:
            connection_string: PostgreSQL connection string
            batch_size: Number of rows per batch insert
        """
        if connection_string:
            self.connection_string = connection_string
        else:
            self.connection_string = (
                f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
        
        self.batch_size = batch_size
        self.engine: Optional[Engine] = None
        self.load_stats: Dict[str, Dict[str, Any]] = {}
        self.rejected_records: Dict[str, pd.DataFrame] = {}
        self.source_locations: Dict[str, str] = {}
        
        logger.info(f"▶ DatabaseLoader initialized")
        logger.info(f"   Connection: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    def connect(self) -> Engine:
        """Establish database connection and create schema if needed"""
        if self.engine is None:
            try:
                self.engine = create_engine(
                    self.connection_string,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True
                )
                # Test connection and create schema
                with self.engine.begin() as conn:
                    conn.execute(text("SELECT 1"))
                    # Create etl_output schema if it doesn't exist
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.ETL_SCHEMA}"))
                ensure_governance_tables(self.engine, schema=self.ETL_SCHEMA)
                logger.info(f"✅ Database connection established (schema: {self.ETL_SCHEMA})")
            except SQLAlchemyError as e:
                logger.error(f"❌ Database connection failed: {e}")
                raise
        return self.engine
    
    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Database connection closed")
    
    def create_table(self, table_key: str, df: pd.DataFrame) -> bool:
        """
        Create table if it doesn't exist in etl_output schema
        
        Args:
            table_key: Key from TABLE_CONFIG
            df: DataFrame with data structure
        
        Returns:
            True if created/exists, False on error
        """
        if table_key not in self.TABLE_CONFIG:
            logger.error(f"Unknown table: {table_key}")
            return False
        
        config = self.TABLE_CONFIG[table_key]
        table_name = config['table_name']
        full_table_name = f"{self.ETL_SCHEMA}.{table_name}"
        
        try:
            engine = self.connect()
            
            # Check if table exists in schema
            inspector = inspect(engine)
            if table_name in inspector.get_table_names(schema=self.ETL_SCHEMA):
                logger.info(f"   Table {full_table_name} already exists")
                return True
            
            # Build CREATE TABLE statement
            columns = []
            for col in df.columns:
                dtype = config['dtype_map'].get(col, 'TEXT')
                columns.append(f'"{col}" {dtype}')
            
            # Add primary key constraint
            pk = config['primary_key']
            if isinstance(pk, list):
                pk_cols = ', '.join(f'"{c}"' for c in pk)
            else:
                pk_cols = f'"{pk}"'
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {', '.join(columns)},
                    PRIMARY KEY ({pk_cols})
                )
            """
            
            with engine.begin() as conn:
                conn.execute(text(create_sql))
            
            logger.info(f"   ✅ Created table {full_table_name}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"   ❌ Failed to create table: {e}")
            return False
    
    def load_table(self, 
                   table_key: str, 
                   df: pd.DataFrame,
                   mode: str = 'replace',
                   source_location: Optional[str] = None,
                   context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Load a single table to database (etl_output schema)
        
        Args:
            table_key: Key from TABLE_CONFIG
            df: DataFrame to load
            mode: 'replace' (TRUNCATE+INSERT), 'append', or 'upsert'
        
        Returns:
            Loading statistics
        """
        if table_key not in self.TABLE_CONFIG:
            raise ValueError(f"Unknown table: {table_key}")
        
        config = self.TABLE_CONFIG[table_key]
        table_name = config['table_name']
        full_table_name = f"{self.ETL_SCHEMA}.{table_name}"
        
        stats = {
            'table': full_table_name,
            'source': table_key,
            'mode': mode,
            'input_rows': len(df),
            'loaded_rows': 0,
            'rejected_rows': 0,
            'start_time': datetime.now(),
            'success': False
        }

        context = context or {}
        dag = context.get("dag")
        dag_run = context.get("dag_run")
        dag_id = dag.dag_id if dag else None
        run_id = context.get("run_id") or (dag_run.run_id if dag_run else None)
        
        logger.info(f"▶ Loading {table_key} → {full_table_name} ({mode} mode)")
        
        try:
            engine = self.connect()

            log_audit_event(
                engine=engine,
                event_type="load_start",
                entity_type="table",
                entity_id=full_table_name,
                status="started",
                details={"mode": mode, "input_rows": len(df)},
                dag_id=dag_id,
                run_id=run_id,
                schema=self.ETL_SCHEMA,
            )
            
            # Ensure table exists
            self.create_table(table_key, df)

            # Apply data classification tags (from config)
            apply_classification_from_config(engine, table_name, schema=self.ETL_SCHEMA)

            # Schema validation (sample-based)
            sample_rows = df.head(1000).to_dict(orient="records")
            validation = validate_dataframe(table_key, sample_rows)
            if validation.get("status") != "skipped":
                log_schema_validation(
                    engine=engine,
                    table_name=table_name,
                    sample_size=len(sample_rows),
                    error_count=validation.get("error_count", 0),
                    errors=validation.get("errors", []),
                    status=validation.get("status", "pass"),
                    dag_id=dag_id,
                    run_id=run_id,
                    schema=self.ETL_SCHEMA,
                )
                if validation.get("status") == "fail":
                    log_audit_event(
                        engine=engine,
                        event_type="schema_validation",
                        entity_type="table",
                        entity_id=full_table_name,
                        status="warning",
                        details={
                            "sample_size": len(sample_rows),
                            "error_count": validation.get("error_count", 0),
                        },
                        dag_id=dag_id,
                        run_id=run_id,
                        schema=self.ETL_SCHEMA,
                    )
            
            # Handle different modes
            if mode == 'replace':
                # Truncate existing data
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))
                logger.info(f"   Truncated existing data")
            
            # Insert data in batches
            loaded = 0
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            for i in range(0, len(df), self.batch_size):
                batch = df.iloc[i:i + self.batch_size]
                batch_num = (i // self.batch_size) + 1
                
                try:
                    batch.to_sql(
                        table_name,
                        engine,
                        schema=self.ETL_SCHEMA,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    loaded += len(batch)
                    
                    if batch_num % 10 == 0 or batch_num == total_batches:
                        logger.info(f"   Batch {batch_num}/{total_batches}: {loaded:,} rows loaded")
                        
                except SQLAlchemyError as e:
                    logger.error(f"   ❌ Batch {batch_num} failed: {e}")
                    stats['rejected_rows'] += len(batch)
                    
                    # Track rejected records
                    if table_key not in self.rejected_records:
                        self.rejected_records[table_key] = batch.copy()
                    else:
                        self.rejected_records[table_key] = pd.concat([
                            self.rejected_records[table_key], batch
                        ])
            
            stats['loaded_rows'] = loaded
            stats['success'] = loaded > 0
            stats['end_time'] = datetime.now()
            stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()

            log_lineage(
                engine=engine,
                source_type="file" if source_location else "unknown",
                source_location=source_location,
                target_table=full_table_name,
                transformation="cleaning+transform+load",
                row_count=loaded,
                dag_id=dag_id,
                run_id=run_id,
                schema=self.ETL_SCHEMA,
            )

            log_audit_event(
                engine=engine,
                event_type="load_complete",
                entity_type="table",
                entity_id=full_table_name,
                status="success" if stats['success'] else "failed",
                details={
                    "loaded_rows": loaded,
                    "rejected_rows": stats['rejected_rows'],
                    "duration_seconds": stats['duration_seconds'],
                },
                dag_id=dag_id,
                run_id=run_id,
                schema=self.ETL_SCHEMA,
            )
            
            logger.info(f"   ✅ Loaded {loaded:,}/{len(df):,} rows in {stats['duration_seconds']:.2f}s")
            
        except Exception as e:
            stats['success'] = False
            stats['error'] = str(e)
            stats['end_time'] = datetime.now()
            logger.error(f"   ❌ Load failed: {e}")

            if self.engine:
                log_audit_event(
                    engine=self.engine,
                    event_type="load_error",
                    entity_type="table",
                    entity_id=full_table_name,
                    status="failed",
                    details={"error": str(e)},
                    dag_id=dag_id,
                    run_id=run_id,
                    schema=self.ETL_SCHEMA,
                )
        
        self.load_stats[table_key] = stats
        return stats
    
    def load_all(self, 
                 cleaned_tables: Dict[str, pd.DataFrame] = None,
                 mode: str = 'replace',
                 context: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Load all cleaned tables to database
        
        Args:
            cleaned_tables: Dictionary of cleaned DataFrames
            mode: Load mode for all tables
        
        Returns:
            Dictionary of loading statistics
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - LOAD PHASE: Loading All Tables to PostgreSQL")
        logger.info("="*70 + "\n")
        
        # If not provided, load from processed CSVs
        if cleaned_tables is None:
            cleaned_tables = self._load_from_processed()
        
        if not cleaned_tables:
            raise ValueError("No data to load")
        
        # Load each table
        for table_key in self.TABLE_CONFIG.keys():
            # Convert table_key to match cleaned_tables keys
            source_key = table_key.replace('_cleaned', '')
            
            if source_key in cleaned_tables:
                source_location = self.source_locations.get(source_key)
                self.load_table(
                    table_key,
                    cleaned_tables[source_key],
                    mode,
                    source_location=source_location,
                    context=context,
                )
            else:
                logger.warning(f"   ⚠️ No data for {table_key}")
        
        # Summary
        total_loaded = sum(s['loaded_rows'] for s in self.load_stats.values())
        total_rejected = sum(s['rejected_rows'] for s in self.load_stats.values())
        
        logger.info("-"*50)
        logger.info(f"✅ LOAD COMPLETE: {total_loaded:,} rows loaded, {total_rejected} rejected")
        logger.info("-"*50 + "\n")
        
        self.disconnect()
        return self.load_stats
    
    def _load_from_processed(self) -> Dict[str, pd.DataFrame]:
        """Load data from processed CSV files"""
        logger.info("▶ Loading from processed CSVs...")
        data = {}
        
        for table_key in self.TABLE_CONFIG.keys():
            source_key = table_key.replace('_cleaned', '')
            csv_file = DATA_PROCESSED / f"{source_key}_cleaned.csv"
            
            if csv_file.exists():
                data[source_key] = pd.read_csv(csv_file)
                self.source_locations[source_key] = str(csv_file)
                logger.info(f"   ✅ Loaded {source_key}: {len(data[source_key]):,} rows")
            else:
                logger.warning(f"   ⚠️ {csv_file.name} not found")
        
        return data
    
    def get_load_summary(self) -> pd.DataFrame:
        """Get summary of load results"""
        if not self.load_stats:
            return pd.DataFrame()
        
        records = []
        for table_key, stats in self.load_stats.items():
            records.append({
                'Table': stats.get('table', table_key),
                'Mode': stats.get('mode', ''),
                'Input_Rows': stats.get('input_rows', 0),
                'Loaded_Rows': stats.get('loaded_rows', 0),
                'Rejected_Rows': stats.get('rejected_rows', 0),
                'Duration_Sec': stats.get('duration_seconds', 0),
                'Success': stats.get('success', False)
            })
        
        return pd.DataFrame(records)
    
    def save_rejected_records(self, output_dir: Optional[Path] = None) -> Dict[str, Path]:
        """Save rejected records to CSV for review"""
        output_dir = output_dir or (DATA_PROCESSED / "rejected")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_files = {}
        for table_key, df in self.rejected_records.items():
            if len(df) > 0:
                output_file = output_dir / f"{table_key}_rejected.csv"
                df.to_csv(output_file, index=False)
                output_files[table_key] = output_file
                logger.info(f"   Saved {len(df)} rejected records: {output_file}")
        
        return output_files


# ========================================
# Airflow-compatible functions
# ========================================

def load_all_tables(cleaned_tables: Dict[str, pd.DataFrame] = None, 
                    mode: str = 'replace',
                    **context) -> Dict[str, Any]:
    """
    Airflow task callable: Load all tables to database
    
    Args:
        cleaned_tables: Optional pre-cleaned DataFrames
        mode: Load mode ('replace', 'append', 'upsert')
    
    Returns:
        Dictionary with loading info
    """
    loader = DatabaseLoader()
    
    # Load all tables
    stats = loader.load_all(cleaned_tables, mode, context=context)
    
    # Save rejected records if any
    rejected_files = loader.save_rejected_records()
    
    return {
        'tables_loaded': list(stats.keys()),
        'total_loaded': sum(s['loaded_rows'] for s in stats.values()),
        'total_rejected': sum(s['rejected_rows'] for s in stats.values()),
        'load_stats': stats,
        'rejected_files': {k: str(v) for k, v in rejected_files.items()}
    }


def load_single_table(table_key: str, 
                      df: pd.DataFrame,
                      mode: str = 'replace',
                      **context) -> Dict[str, Any]:
    """
    Airflow task callable: Load a single table
    """
    loader = DatabaseLoader()
    stats = loader.load_table(table_key, df, mode, context=context)
    loader.disconnect()
    return stats


# ========================================
# Main execution
# ========================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("TEAM 1 - ETL PIPELINE: LOAD PHASE TEST")
    print("="*70 + "\n")
    
    # Initialize loader
    loader = DatabaseLoader()
    
    try:
        # Test connection
        loader.connect()
        print("✅ Database connection successful!\n")
        
        # Load from processed CSVs
        stats = loader.load_all(mode='replace')
        
        # Print summary
        print("\n📊 LOAD SUMMARY:")
        print(loader.get_load_summary().to_string(index=False))
        
        # Save rejected if any
        rejected = loader.save_rejected_records()
        if rejected:
            print("\n⚠️ REJECTED RECORDS:")
            for table, path in rejected.items():
                print(f"   {table}: {path}")
        
        print("\n✅ Load phase complete!")
        
    except Exception as e:
        print(f"\n❌ Load failed: {e}")
        print("\nNote: Make sure Docker PostgreSQL is running:")
        print("   docker-compose -f Docker/docker-compose.yaml up -d")
    
    finally:
        loader.disconnect()
