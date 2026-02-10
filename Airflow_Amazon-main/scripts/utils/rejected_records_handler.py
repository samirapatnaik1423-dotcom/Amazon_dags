# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Rejected Records Handler
# Tasks: T0022
# ═══════════════════════════════════════════════════════════════════════

"""
Rejected Records Handler - Track and store failed records
Part of T0022: Create rejected_records error table

Provides:
- Rejected records table in database
- Capture record details, error type, timestamp
- DAG run tracking for traceability
- Export capabilities (CSV, JSON)
- Query and reporting functions
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Union
from sqlalchemy import create_engine, text, inspect, Column, String, Text, DateTime, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
import json
import hashlib

logger = logging.getLogger(__name__)


class RejectedRecordsHandler:
    """
    TEAM 1 - T0022: Rejected records error table handler
    
    Captures records that fail during ETL processing for:
    - Constraint violations
    - Data quality issues
    - Transformation errors
    - Load failures
    
    Table Schema:
    - id: Auto-increment ID
    - record_id: Original record identifier
    - source_table: Source table name
    - error_type: Type of error (duplicate_key, null_value, etc.)
    - error_message: Detailed error message
    - raw_data: JSON of the rejected row
    - rejected_at: Timestamp of rejection
    - dag_run_id: Airflow DAG run identifier
    - task_id: Airflow task identifier
    - retry_count: Number of retry attempts
    - status: pending, resolved, ignored
    """
    
    # Default Airflow PostgreSQL connection
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    # Table name for rejected records
    TABLE_NAME = "rejected_records"
    SCHEMA = "public"
    
    # Table DDL
    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        id SERIAL PRIMARY KEY,
        record_id VARCHAR(255),
        source_table VARCHAR(100) NOT NULL,
        error_type VARCHAR(100) NOT NULL,
        error_message TEXT,
        raw_data TEXT,
        rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dag_run_id VARCHAR(255),
        task_id VARCHAR(255),
        retry_count INTEGER DEFAULT 0,
        status VARCHAR(50) DEFAULT 'pending',
        record_hash VARCHAR(64),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_rejected_source_table 
        ON {schema}.{table_name}(source_table);
    CREATE INDEX IF NOT EXISTS idx_rejected_error_type 
        ON {schema}.{table_name}(error_type);
    CREATE INDEX IF NOT EXISTS idx_rejected_dag_run 
        ON {schema}.{table_name}(dag_run_id);
    CREATE INDEX IF NOT EXISTS idx_rejected_status 
        ON {schema}.{table_name}(status);
    CREATE INDEX IF NOT EXISTS idx_rejected_at 
        ON {schema}.{table_name}(rejected_at);
    """
    
    def __init__(self,
                 connection_string: Optional[str] = None,
                 table_name: str = "rejected_records",
                 schema: str = "public"):
        """
        Initialize rejected records handler
        
        Args:
            connection_string: Database connection string
            table_name: Name for rejected records table
            schema: Database schema
        """
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.table_name = table_name
        self.schema = schema
        self.engine: Optional[Engine] = None
        self._table_created = False
        
        logger.info(f"▶ TEAM 1 - T0022: RejectedRecordsHandler initialized")
    
    def connect(self) -> Engine:
        """Establish database connection"""
        if self.engine is None:
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)
        return self.engine
    
    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
    
    # ========================================
    # Team 1 - T0022: Error Table Creation (Rejects)
    # ========================================
    def ensure_table_exists(self) -> bool:
        """
        Create rejected_records table if it doesn't exist
        
        Returns:
            True if table exists or was created
        """
        if self._table_created:
            return True
        
        engine = self.connect()
        
        try:
            with engine.connect() as conn:
                conn.execute(text(
                    self.CREATE_TABLE_SQL.format(
                        schema=self.schema,
                        table_name=self.table_name
                    )
                ))
                conn.commit()
            
            self._table_created = True
            logger.info(f"✅ Table {self.schema}.{self.table_name} ready")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to create rejected_records table: {e}")
            return False
    
    def add_rejected_record(self,
                            record_id: Any,
                            source_table: str,
                            error_type: str,
                            error_message: str,
                            raw_data: Union[Dict, pd.Series],
                            dag_run_id: Optional[str] = None,
                            task_id: Optional[str] = None) -> bool:
        """
        Add a single rejected record to the error table
        
        Args:
            record_id: Original record identifier
            source_table: Source table name
            error_type: Type of error (duplicate_key, null_value, constraint_violation, etc.)
            error_message: Detailed error description
            raw_data: The rejected record data (dict or pandas Series)
            dag_run_id: Optional Airflow DAG run ID
            task_id: Optional Airflow task ID
        
        Returns:
            True if record was added successfully
        """
        self.ensure_table_exists()
        engine = self.connect()
        
        # Convert raw_data to JSON string
        if isinstance(raw_data, pd.Series):
            raw_data = raw_data.to_dict()
        raw_data_json = json.dumps(raw_data, default=str)
        
        # Generate hash for deduplication
        record_hash = hashlib.md5(
            f"{source_table}:{record_id}:{raw_data_json}".encode()
        ).hexdigest()
        
        try:
            with engine.connect() as conn:
                conn.execute(text(f"""
                    INSERT INTO {self.schema}.{self.table_name}
                    (record_id, source_table, error_type, error_message, 
                     raw_data, dag_run_id, task_id, record_hash)
                    VALUES (:record_id, :source_table, :error_type, :error_message,
                            :raw_data, :dag_run_id, :task_id, :record_hash)
                """), {
                    'record_id': str(record_id),
                    'source_table': source_table,
                    'error_type': error_type,
                    'error_message': error_message[:2000],  # Truncate if too long
                    'raw_data': raw_data_json,
                    'dag_run_id': dag_run_id,
                    'task_id': task_id,
                    'record_hash': record_hash
                })
                conn.commit()
            
            logger.debug(f"   Added rejected record: {source_table}/{record_id}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to add rejected record: {e}")
            return False
    
    def add_rejected_batch(self,
                           df: pd.DataFrame,
                           source_table: str,
                           error_type: str,
                           error_message: str,
                           record_id_column: Optional[str] = None,
                           dag_run_id: Optional[str] = None,
                           task_id: Optional[str] = None) -> int:
        """
        Add multiple rejected records at once
        
        Args:
            df: DataFrame of rejected records
            source_table: Source table name
            error_type: Type of error (same for all records)
            error_message: Error message template
            record_id_column: Column to use as record_id
            dag_run_id: Optional DAG run ID
            task_id: Optional task ID
        
        Returns:
            Number of records added
        """
        if df.empty:
            return 0
        
        self.ensure_table_exists()
        engine = self.connect()
        
        added_count = 0
        
        try:
            records_to_insert = []
            
            for idx, row in df.iterrows():
                record_id = row.get(record_id_column, idx) if record_id_column else idx
                raw_data_json = json.dumps(row.to_dict(), default=str)
                record_hash = hashlib.md5(
                    f"{source_table}:{record_id}:{raw_data_json}".encode()
                ).hexdigest()
                
                records_to_insert.append({
                    'record_id': str(record_id),
                    'source_table': source_table,
                    'error_type': error_type,
                    'error_message': error_message[:2000],
                    'raw_data': raw_data_json,
                    'dag_run_id': dag_run_id,
                    'task_id': task_id,
                    'record_hash': record_hash
                })
            
            # Bulk insert
            with engine.connect() as conn:
                for record in records_to_insert:
                    conn.execute(text(f"""
                        INSERT INTO {self.schema}.{self.table_name}
                        (record_id, source_table, error_type, error_message,
                         raw_data, dag_run_id, task_id, record_hash)
                        VALUES (:record_id, :source_table, :error_type, :error_message,
                                :raw_data, :dag_run_id, :task_id, :record_hash)
                    """), record)
                    added_count += 1
                
                conn.commit()
            
            logger.info(f"✅ Added {added_count} rejected records from {source_table}")
            return added_count
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to add rejected batch: {e}")
            return added_count
    
    def get_rejected_records(self,
                             source_table: Optional[str] = None,
                             error_type: Optional[str] = None,
                             dag_run_id: Optional[str] = None,
                             status: Optional[str] = None,
                             limit: int = 1000) -> pd.DataFrame:
        """
        Query rejected records with optional filters
        
        Args:
            source_table: Filter by source table
            error_type: Filter by error type
            dag_run_id: Filter by DAG run ID
            status: Filter by status (pending, resolved, ignored)
            limit: Maximum records to return
        
        Returns:
            DataFrame of rejected records
        """
        self.ensure_table_exists()
        engine = self.connect()
        
        # Build query
        query = f"SELECT * FROM {self.schema}.{self.table_name} WHERE 1=1"
        params = {}
        
        if source_table:
            query += " AND source_table = :source_table"
            params['source_table'] = source_table
        
        if error_type:
            query += " AND error_type = :error_type"
            params['error_type'] = error_type
        
        if dag_run_id:
            query += " AND dag_run_id = :dag_run_id"
            params['dag_run_id'] = dag_run_id
        
        if status:
            query += " AND status = :status"
            params['status'] = status
        
        query += f" ORDER BY rejected_at DESC LIMIT {limit}"
        
        try:
            return pd.read_sql(text(query), engine, params=params)
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to query rejected records: {e}")
            return pd.DataFrame()
    
    def get_rejection_summary(self,
                              dag_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get summary statistics of rejected records
        
        Args:
            dag_run_id: Optional filter by DAG run
        
        Returns:
            Summary dictionary with counts and breakdowns
        """
        self.ensure_table_exists()
        engine = self.connect()
        
        summary = {
            'total_rejected': 0,
            'by_source_table': {},
            'by_error_type': {},
            'by_status': {},
            'recent_rejections': []
        }
        
        try:
            with engine.connect() as conn:
                # Total count
                query = f"SELECT COUNT(*) FROM {self.schema}.{self.table_name}"
                if dag_run_id:
                    query += f" WHERE dag_run_id = '{dag_run_id}'"
                result = conn.execute(text(query))
                summary['total_rejected'] = result.scalar()
                
                # By source table
                query = f"""
                    SELECT source_table, COUNT(*) as count 
                    FROM {self.schema}.{self.table_name}
                """
                if dag_run_id:
                    query += f" WHERE dag_run_id = '{dag_run_id}'"
                query += " GROUP BY source_table"
                result = conn.execute(text(query))
                summary['by_source_table'] = {row[0]: row[1] for row in result}
                
                # By error type
                query = f"""
                    SELECT error_type, COUNT(*) as count 
                    FROM {self.schema}.{self.table_name}
                """
                if dag_run_id:
                    query += f" WHERE dag_run_id = '{dag_run_id}'"
                query += " GROUP BY error_type"
                result = conn.execute(text(query))
                summary['by_error_type'] = {row[0]: row[1] for row in result}
                
                # By status
                query = f"""
                    SELECT status, COUNT(*) as count 
                    FROM {self.schema}.{self.table_name}
                """
                if dag_run_id:
                    query += f" WHERE dag_run_id = '{dag_run_id}'"
                query += " GROUP BY status"
                result = conn.execute(text(query))
                summary['by_status'] = {row[0]: row[1] for row in result}
            
            logger.info(f"✅ Rejection summary: {summary['total_rejected']} total records")
            return summary
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to get rejection summary: {e}")
            return summary
    
    def update_status(self,
                      record_ids: List[int],
                      new_status: str) -> int:
        """
        Update status of rejected records
        
        Args:
            record_ids: List of rejected record IDs
            new_status: New status (pending, resolved, ignored)
        
        Returns:
            Number of records updated
        """
        if not record_ids:
            return 0
        
        engine = self.connect()
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET status = :status, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(:ids)
                """), {'status': new_status, 'ids': record_ids})
                conn.commit()
                
                updated = result.rowcount
                logger.info(f"✅ Updated {updated} records to status: {new_status}")
                return updated
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to update status: {e}")
            return 0
    
    def retry_rejected_record(self,
                              record_id: int,
                              max_retries: int = 3) -> bool:
        """
        Increment retry count for a rejected record
        
        Args:
            record_id: ID of the rejected record
            max_retries: Maximum retry attempts before marking as failed
        
        Returns:
            True if can retry, False if max retries exceeded
        """
        engine = self.connect()
        
        try:
            with engine.connect() as conn:
                # Get current retry count
                result = conn.execute(text(f"""
                    SELECT retry_count FROM {self.schema}.{self.table_name}
                    WHERE id = :id
                """), {'id': record_id})
                row = result.fetchone()
                
                if not row:
                    return False
                
                current_retries = row[0]
                
                if current_retries >= max_retries:
                    # Mark as failed
                    conn.execute(text(f"""
                        UPDATE {self.schema}.{self.table_name}
                        SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                        WHERE id = :id
                    """), {'id': record_id})
                    conn.commit()
                    logger.warning(f"   Record {record_id} exceeded max retries")
                    return False
                
                # Increment retry count
                conn.execute(text(f"""
                    UPDATE {self.schema}.{self.table_name}
                    SET retry_count = retry_count + 1, updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """), {'id': record_id})
                conn.commit()
                
                logger.info(f"   Record {record_id} retry {current_retries + 1}/{max_retries}")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to retry record: {e}")
            return False
    
    def export_to_csv(self,
                      filepath: str,
                      source_table: Optional[str] = None,
                      dag_run_id: Optional[str] = None) -> bool:
        """
        Export rejected records to CSV file
        
        Args:
            filepath: Output file path
            source_table: Optional filter by source table
            dag_run_id: Optional filter by DAG run
        
        Returns:
            True if export successful
        """
        df = self.get_rejected_records(
            source_table=source_table,
            dag_run_id=dag_run_id,
            limit=100000  # High limit for export
        )
        
        if df.empty:
            logger.warning("⚠️ No rejected records to export")
            return False
        
        try:
            df.to_csv(filepath, index=False)
            logger.info(f"✅ Exported {len(df)} rejected records to {filepath}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to export: {e}")
            return False
    
    def clear_resolved(self, days_old: int = 30) -> int:
        """
        Delete resolved/ignored records older than specified days
        
        Args:
            days_old: Delete records older than this many days
        
        Returns:
            Number of records deleted
        """
        engine = self.connect()
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"""
                    DELETE FROM {self.schema}.{self.table_name}
                    WHERE status IN ('resolved', 'ignored')
                    AND rejected_at < CURRENT_TIMESTAMP - INTERVAL '{days_old} days'
                """))
                conn.commit()
                
                deleted = result.rowcount
                logger.info(f"✅ Deleted {deleted} old resolved/ignored records")
                return deleted
                
        except SQLAlchemyError as e:
            logger.error(f"❌ Failed to clear old records: {e}")
            return 0
    
    def get_pending_count(self, source_table: Optional[str] = None) -> int:
        """Get count of pending rejected records"""
        engine = self.connect()
        
        try:
            query = f"""
                SELECT COUNT(*) FROM {self.schema}.{self.table_name}
                WHERE status = 'pending'
            """
            if source_table:
                query += f" AND source_table = '{source_table}'"
            
            with engine.connect() as conn:
                result = conn.execute(text(query))
                return result.scalar()
                
        except SQLAlchemyError:
            return 0


# ========================================
# Convenience functions
# ========================================

def log_rejection(record_id: Any,
                  source_table: str,
                  error_type: str,
                  error_message: str,
                  raw_data: Dict,
                  dag_run_id: Optional[str] = None) -> bool:
    """
    Log a single rejected record (convenience function)
    
    Args:
        record_id: Record identifier
        source_table: Source table name
        error_type: Type of error
        error_message: Error description
        raw_data: The rejected record
        dag_run_id: Optional DAG run ID
    
    Returns:
        True if logged successfully
    """
    handler = RejectedRecordsHandler()
    try:
        return handler.add_rejected_record(
            record_id, source_table, error_type, 
            error_message, raw_data, dag_run_id
        )
    finally:
        handler.disconnect()


def get_rejection_stats(dag_run_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get rejection statistics (convenience function)
    
    Args:
        dag_run_id: Optional DAG run filter
    
    Returns:
        Summary statistics
    """
    handler = RejectedRecordsHandler()
    try:
        return handler.get_rejection_summary(dag_run_id)
    finally:
        handler.disconnect()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    handler = RejectedRecordsHandler()
    
    # Ensure table exists
    handler.ensure_table_exists()
    
    # Add a test rejection
    handler.add_rejected_record(
        record_id="12345",
        source_table="sales",
        error_type="duplicate_key",
        error_message="Duplicate Order Number: 12345",
        raw_data={'Order Number': 12345, 'Amount': 100.00},
        dag_run_id="manual__2026-01-14T120000"
    )
    
    # Get summary
    summary = handler.get_rejection_summary()
    print(f"\nRejection summary: {summary}")
    
    # Get pending records
    pending = handler.get_rejected_records(status='pending')
    print(f"\nPending records: {len(pending)}")
    
    handler.disconnect()
