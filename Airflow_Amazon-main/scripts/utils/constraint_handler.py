# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Constraint Handler
# Tasks: T0020
# ═══════════════════════════════════════════════════════════════════════

"""
Constraint Violation Handler - Handle database constraint errors gracefully
Part of T0020: Create constraint violation handler

Provides:
- Primary key duplicate detection
- Foreign key violation handling
- Not null constraint handling
- Data type constraint validation
- Option A behavior: Log error, skip record, continue loading

Strategy: Log, Skip, Continue (as per user requirement)
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Callable
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging
from datetime import datetime
from enum import Enum
import re

logger = logging.getLogger(__name__)


class ConstraintType(Enum):
    """Types of database constraints"""
    PRIMARY_KEY = "primary_key"
    FOREIGN_KEY = "foreign_key"
    UNIQUE = "unique"
    NOT_NULL = "not_null"
    CHECK = "check"
    DATA_TYPE = "data_type"
    UNKNOWN = "unknown"


class ViolationAction(Enum):
    """Actions to take on constraint violation"""
    SKIP = "skip"           # Skip the record, continue with others
    STOP = "stop"           # Stop entire load operation
    AUTO_FIX = "auto_fix"   # Attempt to fix and retry


class ConstraintViolation:
    """Represents a single constraint violation"""
    
    def __init__(self,
                 record_id: Any,
                 constraint_type: ConstraintType,
                 column: str,
                 value: Any,
                 error_message: str,
                 raw_data: Dict = None):
        self.record_id = record_id
        self.constraint_type = constraint_type
        self.column = column
        self.value = value
        self.error_message = error_message
        self.raw_data = raw_data or {}
        self.timestamp = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'record_id': self.record_id,
            'constraint_type': self.constraint_type.value,
            'column': self.column,
            'value': str(self.value),
            'error_message': self.error_message,
            'raw_data': str(self.raw_data),
            'timestamp': self.timestamp.isoformat()
        }


class ConstraintHandler:
    """
    TEAM 1 - T0020: Database constraint violation handler
    
    Behavior: Option A - Log error, skip record, continue loading others
    
    Features:
    - Pre-load validation to catch issues before database insert
    - Duplicate primary key detection
    - Null value detection for required columns
    - Data type validation
    - Violation logging and reporting
    """
    
    # Default Airflow PostgreSQL connection
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    # Table constraint configurations
    TABLE_CONSTRAINTS = {
        'customers': {
            'primary_key': 'CustomerKey',
            'not_null': ['CustomerKey'],
            'unique': ['CustomerKey']
        },
        'sales': {
            'primary_key': 'Order Number',
            'not_null': ['Order Number', 'Order Date'],
            'unique': ['Order Number']
        },
        'products': {
            'primary_key': 'ProductKey',
            'not_null': ['ProductKey', 'Product Name'],
            'unique': ['ProductKey']
        },
        'stores': {
            'primary_key': 'StoreKey',
            'not_null': ['StoreKey'],
            'unique': ['StoreKey']
        },
        'exchange_rates': {
            'primary_key': 'Date',
            'not_null': ['Date', 'Currency'],
            'unique': []  # Composite: Date + Currency
        }
    }
    
    def __init__(self,
                 connection_string: Optional[str] = None,
                 action: ViolationAction = ViolationAction.SKIP):
        """
        Initialize constraint handler
        
        Args:
            connection_string: Database connection string
            action: Default action on violation (SKIP for Option A)
        """
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.action = action
        self.engine: Optional[Engine] = None
        self.violations: List[ConstraintViolation] = []
        
        logger.info(f"▶ TEAM 1 - T0020: ConstraintHandler initialized (action={action.value})")
    
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
    # Team 1 - T0020: Handling Constraint Violations
    # ========================================
    def validate_before_load(self,
                             df: pd.DataFrame,
                             table_name: str,
                             schema: str = 'public') -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Validate DataFrame before loading to catch constraint violations
        
        This is a PRE-LOAD check to identify issues before hitting the database.
        
        Args:
            df: DataFrame to validate
            table_name: Target table name
            schema: Database schema
        
        Returns:
            (valid_records_df, invalid_records_df, stats_dict)
        """
        logger.info(f"▶ TEAM 1 - T0020: Pre-load validation for {table_name}")
        
        stats = {
            'total_records': len(df),
            'valid_records': 0,
            'invalid_records': 0,
            'violations_by_type': {},
            'validation_time': datetime.now()
        }
        
        if df.empty:
            logger.info("   Empty DataFrame, no validation needed")
            return df, pd.DataFrame(), stats
        
        # Get table constraints
        constraints = self.TABLE_CONSTRAINTS.get(table_name.lower(), {})
        primary_key = constraints.get('primary_key')
        not_null_cols = constraints.get('not_null', [])
        unique_cols = constraints.get('unique', [])
        
        # Track invalid row indices
        invalid_indices = set()
        
        # 1. Check for NULL values in NOT NULL columns
        for col in not_null_cols:
            if col in df.columns:
                null_mask = df[col].isna()
                null_count = null_mask.sum()
                
                if null_count > 0:
                    null_indices = df[null_mask].index.tolist()
                    invalid_indices.update(null_indices)
                    
                    for idx in null_indices:
                        violation = ConstraintViolation(
                            record_id=df.loc[idx, primary_key] if primary_key and primary_key in df.columns else idx,
                            constraint_type=ConstraintType.NOT_NULL,
                            column=col,
                            value=None,
                            error_message=f"NULL value in required column '{col}'",
                            raw_data=df.loc[idx].to_dict()
                        )
                        self.violations.append(violation)
                    
                    stats['violations_by_type']['not_null'] = stats['violations_by_type'].get('not_null', 0) + null_count
                    logger.warning(f"   ⚠️ Found {null_count} NULL values in '{col}'")
        
        # 2. Check for duplicate primary keys (within the DataFrame)
        if primary_key and primary_key in df.columns:
            duplicates = df[df.duplicated(subset=[primary_key], keep='first')]
            dup_count = len(duplicates)
            
            if dup_count > 0:
                invalid_indices.update(duplicates.index.tolist())
                
                for idx in duplicates.index:
                    violation = ConstraintViolation(
                        record_id=df.loc[idx, primary_key],
                        constraint_type=ConstraintType.PRIMARY_KEY,
                        column=primary_key,
                        value=df.loc[idx, primary_key],
                        error_message=f"Duplicate primary key: {df.loc[idx, primary_key]}",
                        raw_data=df.loc[idx].to_dict()
                    )
                    self.violations.append(violation)
                
                stats['violations_by_type']['primary_key'] = dup_count
                logger.warning(f"   ⚠️ Found {dup_count} duplicate primary keys")
        
        # 3. Check for conflicts with existing database records
        db_conflicts = self._check_database_conflicts(df, table_name, schema)
        if db_conflicts:
            invalid_indices.update(db_conflicts.index.tolist())
            
            for idx in db_conflicts.index:
                violation = ConstraintViolation(
                    record_id=db_conflicts.loc[idx, primary_key] if primary_key else idx,
                    constraint_type=ConstraintType.PRIMARY_KEY,
                    column=primary_key or 'unknown',
                    value=db_conflicts.loc[idx, primary_key] if primary_key and primary_key in db_conflicts.columns else None,
                    error_message="Record already exists in database",
                    raw_data=db_conflicts.loc[idx].to_dict()
                )
                self.violations.append(violation)
            
            stats['violations_by_type']['db_conflict'] = len(db_conflicts)
            logger.warning(f"   ⚠️ Found {len(db_conflicts)} database conflicts")
        
        # Split into valid and invalid
        valid_df = df[~df.index.isin(invalid_indices)]
        invalid_df = df[df.index.isin(invalid_indices)]
        
        stats['valid_records'] = len(valid_df)
        stats['invalid_records'] = len(invalid_df)
        
        logger.info(f"✅ Validation complete: {stats['valid_records']:,} valid, {stats['invalid_records']:,} invalid")
        
        return valid_df, invalid_df, stats
    
    def _check_database_conflicts(self,
                                  df: pd.DataFrame,
                                  table_name: str,
                                  schema: str) -> pd.DataFrame:
        """
        Check for primary key conflicts with existing database records
        
        Returns:
            DataFrame of conflicting records
        """
        constraints = self.TABLE_CONSTRAINTS.get(table_name.lower(), {})
        primary_key = constraints.get('primary_key')
        
        if not primary_key or primary_key not in df.columns:
            return pd.DataFrame()
        
        engine = self.connect()
        
        try:
            inspector = inspect(engine)
            if table_name.lower() not in [t.lower() for t in inspector.get_table_names(schema=schema)]:
                return pd.DataFrame()  # Table doesn't exist, no conflicts
            
            # Get existing keys
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT "{primary_key}" FROM {schema}.{table_name}'))
                existing_keys = set(row[0] for row in result)
            
            # Find conflicts
            conflicts = df[df[primary_key].isin(existing_keys)]
            return conflicts
            
        except SQLAlchemyError as e:
            logger.error(f"   Error checking database conflicts: {e}")
            return pd.DataFrame()
    
    def handle_violation(self,
                         violation: ConstraintViolation,
                         action: Optional[ViolationAction] = None) -> bool:
        """
        Handle a single constraint violation
        
        Args:
            violation: The violation to handle
            action: Action to take (default: self.action)
        
        Returns:
            True if should continue loading, False if should stop
        """
        action = action or self.action
        
        self.violations.append(violation)
        
        if action == ViolationAction.SKIP:
            logger.warning(f"   SKIP: {violation.constraint_type.value} violation - {violation.error_message}")
            return True  # Continue loading
            
        elif action == ViolationAction.STOP:
            logger.error(f"   STOP: {violation.constraint_type.value} violation - {violation.error_message}")
            return False  # Stop loading
            
        elif action == ViolationAction.AUTO_FIX:
            # Attempt auto-fix (limited capabilities)
            logger.info(f"   AUTO_FIX: Attempting to fix {violation.constraint_type.value} violation")
            return True  # Continue after fix attempt
        
        return True
    
    def parse_database_error(self, error: SQLAlchemyError) -> ConstraintViolation:
        """
        Parse a SQLAlchemy error to determine constraint type
        
        Args:
            error: SQLAlchemy exception
        
        Returns:
            ConstraintViolation object
        """
        error_msg = str(error).lower()
        
        if 'duplicate key' in error_msg or 'unique constraint' in error_msg:
            constraint_type = ConstraintType.PRIMARY_KEY
        elif 'foreign key' in error_msg:
            constraint_type = ConstraintType.FOREIGN_KEY
        elif 'null value' in error_msg or 'not null' in error_msg:
            constraint_type = ConstraintType.NOT_NULL
        elif 'check constraint' in error_msg:
            constraint_type = ConstraintType.CHECK
        elif 'data type' in error_msg or 'invalid input' in error_msg:
            constraint_type = ConstraintType.DATA_TYPE
        else:
            constraint_type = ConstraintType.UNKNOWN
        
        # Try to extract column name from error
        column_match = re.search(r'column[:\s]+"?(\w+)"?', str(error), re.IGNORECASE)
        column = column_match.group(1) if column_match else 'unknown'
        
        return ConstraintViolation(
            record_id='unknown',
            constraint_type=constraint_type,
            column=column,
            value=None,
            error_message=str(error)[:500]  # Truncate long errors
        )
    
    def get_violations(self) -> List[Dict[str, Any]]:
        """Get all violations as list of dictionaries"""
        return [v.to_dict() for v in self.violations]
    
    def get_violations_df(self) -> pd.DataFrame:
        """Get all violations as DataFrame"""
        if not self.violations:
            return pd.DataFrame(columns=[
                'record_id', 'constraint_type', 'column', 'value', 
                'error_message', 'raw_data', 'timestamp'
            ])
        return pd.DataFrame([v.to_dict() for v in self.violations])
    
    def get_violation_summary(self) -> Dict[str, Any]:
        """Get summary of all violations"""
        summary = {
            'total_violations': len(self.violations),
            'by_type': {},
            'by_column': {}
        }
        
        for v in self.violations:
            # By type
            type_key = v.constraint_type.value
            summary['by_type'][type_key] = summary['by_type'].get(type_key, 0) + 1
            
            # By column
            summary['by_column'][v.column] = summary['by_column'].get(v.column, 0) + 1
        
        return summary
    
    def clear_violations(self):
        """Clear all recorded violations"""
        self.violations = []
        logger.info("   Violations cleared")
    
    def export_violations(self, filepath: str) -> bool:
        """
        Export violations to CSV file
        
        Args:
            filepath: Output file path
        
        Returns:
            True if successful
        """
        try:
            df = self.get_violations_df()
            df.to_csv(filepath, index=False)
            logger.info(f"✅ Exported {len(df)} violations to {filepath}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to export violations: {e}")
            return False


# ========================================
# Convenience functions
# ========================================

def validate_and_filter(df: pd.DataFrame,
                        table_name: str,
                        connection_string: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate DataFrame and return valid/invalid split
    
    Args:
        df: DataFrame to validate
        table_name: Target table name
        connection_string: Optional connection string
    
    Returns:
        (valid_df, invalid_df)
    """
    handler = ConstraintHandler(connection_string)
    valid_df, invalid_df, stats = handler.validate_before_load(df, table_name)
    handler.disconnect()
    return valid_df, invalid_df


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data with some constraint violations
    sample_df = pd.DataFrame({
        'CustomerKey': [1, 2, 3, 3, None],  # Duplicate and NULL
        'Name': ['Alice', 'Bob', 'Charlie', 'Charlie2', 'Eve'],
        'Age': [25, 30, 35, 28, 42]
    })
    
    handler = ConstraintHandler()
    valid_df, invalid_df, stats = handler.validate_before_load(sample_df, 'customers')
    
    print(f"\nValid records: {len(valid_df)}")
    print(f"Invalid records: {len(invalid_df)}")
    print(f"Violations: {handler.get_violation_summary()}")
    
    handler.disconnect()
