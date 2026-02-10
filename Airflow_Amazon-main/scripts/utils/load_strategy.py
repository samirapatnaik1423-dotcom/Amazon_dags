# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Load Strategy Utility
# Tasks: T0019
# ═══════════════════════════════════════════════════════════════════════

"""
Load Strategy Utility - Full vs Incremental Loading
Part of T0019: Build incremental load strategy

Provides:
- Full load (truncate and reload)
- Incremental load (new records only)
- Primary key-based comparison
- Date-based filtering for Sales table
- Load tracking and metadata
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Union
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class LoadType(Enum):
    """Load strategy types"""
    FULL = "full"           # Truncate and reload all data
    INCREMENTAL = "incremental"  # Load only new/changed records
    APPEND = "append"       # Append without checking duplicates


class LoadStrategy:
    """
    TEAM 1 - T0019: Data loading strategy manager
    
    Determines whether to do full or incremental load based on:
    - Table configuration
    - Primary key comparison
    - Optional date-based filtering
    
    Tables Configuration:
    - Customers: Primary key = CustomerKey
    - Sales: Primary key = Order Number, optional date filter on Order Date
    - Products: Primary key = ProductKey
    - Stores: Primary key = StoreKey
    - Exchange_Rates: Primary key = Date (per currency)
    """
    
    # Default Airflow PostgreSQL connection
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    # Table configurations with primary keys
    TABLE_CONFIGS = {
        'customers': {
            'primary_key': 'CustomerKey',
            'date_column': None,
            'supports_incremental': True
        },
        'sales': {
            'primary_key': 'Order Number',
            'date_column': 'Order Date',
            'supports_incremental': True
        },
        'products': {
            'primary_key': 'ProductKey',
            'date_column': None,
            'supports_incremental': True
        },
        'stores': {
            'primary_key': 'StoreKey',
            'date_column': None,
            'supports_incremental': True
        },
        'exchange_rates': {
            'primary_key': 'Date',
            'date_column': 'Date',
            'supports_incremental': True
        }
    }
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize load strategy manager
        
        Args:
            connection_string: Database connection string
        """
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.engine: Optional[Engine] = None
        self.load_history: List[Dict] = []
        
        logger.info("▶ TEAM 1 - T0019: LoadStrategy initialized")
    
    def connect(self) -> Engine:
        """Establish database connection"""
        if self.engine is None:
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True
            )
        return self.engine
    
    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
    
    # ========================================
    # Team 1 - T0019: Incremental vs Full Loads
    # ========================================
    def determine_load_type(self,
                            table_name: str,
                            force_full: bool = False,
                            force_incremental: bool = False) -> LoadType:
        """
        Determine the appropriate load type for a table
        
        Args:
            table_name: Name of the table
            force_full: Force full load regardless of configuration
            force_incremental: Force incremental load
        
        Returns:
            LoadType enum value
        """
        if force_full:
            logger.info(f"   {table_name}: Forced FULL load")
            return LoadType.FULL
        
        if force_incremental:
            logger.info(f"   {table_name}: Forced INCREMENTAL load")
            return LoadType.INCREMENTAL
        
        # Check if table exists
        engine = self.connect()
        inspector = inspect(engine)
        table_exists = table_name.lower() in [t.lower() for t in inspector.get_table_names(schema='public')]
        
        if not table_exists:
            logger.info(f"   {table_name}: Table doesn't exist, using FULL load")
            return LoadType.FULL
        
        # Check table config
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        if not config.get('supports_incremental', False):
            logger.info(f"   {table_name}: Incremental not supported, using FULL load")
            return LoadType.FULL
        
        # Default to incremental for existing tables with config
        logger.info(f"   {table_name}: Using INCREMENTAL load")
        return LoadType.INCREMENTAL
    
    def get_new_records(self,
                        df: pd.DataFrame,
                        table_name: str,
                        schema: str = 'public',
                        date_filter: Optional[datetime] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Get only new records not already in database
        
        Compares by primary key to find records that don't exist in target table.
        
        Args:
            df: Source DataFrame with all records
            table_name: Target table name
            schema: Database schema
            date_filter: Optional date to filter records >= this date (for Sales)
        
        Returns:
            (new_records_df, stats_dict)
        """
        logger.info(f"▶ TEAM 1 - T0019: Finding new records for {table_name}")
        
        stats = {
            'source_rows': len(df),
            'existing_rows': 0,
            'new_rows': 0,
            'filtered_by_date': 0
        }
        
        if df.empty:
            logger.warning("⚠️ Source DataFrame is empty")
            return df, stats
        
        # Get table configuration
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        primary_key = config.get('primary_key')
        date_column = config.get('date_column')
        
        if not primary_key:
            logger.warning(f"⚠️ No primary key configured for {table_name}, returning all records")
            stats['new_rows'] = len(df)
            return df, stats
        
        if primary_key not in df.columns:
            logger.warning(f"⚠️ Primary key '{primary_key}' not in DataFrame, returning all records")
            stats['new_rows'] = len(df)
            return df, stats
        
        engine = self.connect()
        
        try:
            # Check if table exists
            inspector = inspect(engine)
            if table_name.lower() not in [t.lower() for t in inspector.get_table_names(schema=schema)]:
                logger.info(f"   Table {table_name} doesn't exist, all records are new")
                stats['new_rows'] = len(df)
                return df, stats
            
            # Get existing primary keys from database
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT "{primary_key}" FROM {schema}.{table_name}'))
                existing_keys = set(row[0] for row in result)
            
            stats['existing_rows'] = len(existing_keys)
            
            # Apply date filter if specified (for Sales table)
            df_filtered = df.copy()
            if date_filter and date_column and date_column in df.columns:
                df_filtered[date_column] = pd.to_datetime(df_filtered[date_column], errors='coerce')
                before_filter = len(df_filtered)
                df_filtered = df_filtered[df_filtered[date_column] >= date_filter]
                stats['filtered_by_date'] = before_filter - len(df_filtered)
                logger.info(f"   Date filter: {stats['filtered_by_date']} records excluded (< {date_filter.date()})")
            
            # Find new records (primary key not in existing)
            new_records = df_filtered[~df_filtered[primary_key].isin(existing_keys)]
            stats['new_rows'] = len(new_records)
            
            logger.info(f"✅ Incremental analysis: {stats['source_rows']:,} source → {stats['new_rows']:,} new records")
            logger.info(f"   Existing in DB: {stats['existing_rows']:,}, Skipped: {stats['source_rows'] - stats['new_rows']:,}")
            
            return new_records, stats
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Error checking existing records: {e}")
            # Return all records on error (safe fallback)
            stats['new_rows'] = len(df)
            stats['error'] = str(e)
            return df, stats
    
    def get_modified_records(self,
                             df: pd.DataFrame,
                             table_name: str,
                             schema: str = 'public',
                             compare_columns: Optional[List[str]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Get records that have been modified (for upsert operations)
        
        Compares specified columns to detect changes.
        
        Args:
            df: Source DataFrame
            table_name: Target table name
            schema: Database schema
            compare_columns: Columns to compare for changes (default: all non-key columns)
        
        Returns:
            (modified_records_df, stats_dict)
        """
        logger.info(f"▶ TEAM 1 - T0019: Finding modified records for {table_name}")
        
        stats = {
            'source_rows': len(df),
            'modified_rows': 0,
            'unchanged_rows': 0
        }
        
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        primary_key = config.get('primary_key')
        
        if not primary_key or primary_key not in df.columns:
            logger.warning(f"⚠️ Cannot detect modifications without primary key")
            return pd.DataFrame(), stats
        
        engine = self.connect()
        
        try:
            # Load existing data
            existing_df = pd.read_sql(f'SELECT * FROM {schema}.{table_name}', engine)
            
            if existing_df.empty:
                logger.info("   Target table is empty, no modifications to detect")
                return pd.DataFrame(), stats
            
            # Merge source with existing on primary key
            merged = df.merge(
                existing_df,
                on=primary_key,
                how='inner',
                suffixes=('_new', '_old')
            )
            
            # Determine columns to compare
            if compare_columns is None:
                compare_columns = [c for c in df.columns if c != primary_key]
            
            # Find rows with differences
            modified_mask = pd.Series(False, index=merged.index)
            for col in compare_columns:
                new_col = f'{col}_new' if f'{col}_new' in merged.columns else col
                old_col = f'{col}_old' if f'{col}_old' in merged.columns else col
                
                if new_col in merged.columns and old_col in merged.columns:
                    # Compare values (handle NaN)
                    col_diff = (merged[new_col].fillna('__NULL__') != merged[old_col].fillna('__NULL__'))
                    modified_mask = modified_mask | col_diff
            
            modified_keys = merged.loc[modified_mask, primary_key].tolist()
            modified_records = df[df[primary_key].isin(modified_keys)]
            
            stats['modified_rows'] = len(modified_records)
            stats['unchanged_rows'] = len(merged) - stats['modified_rows']
            
            logger.info(f"✅ Modification analysis: {stats['modified_rows']:,} modified, {stats['unchanged_rows']:,} unchanged")
            
            return modified_records, stats
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Error detecting modifications: {e}")
            return pd.DataFrame(), stats
    
    def execute_load(self,
                     df: pd.DataFrame,
                     table_name: str,
                     load_type: LoadType,
                     schema: str = 'public',
                     date_filter: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Execute the load based on strategy
        
        Args:
            df: Source DataFrame
            table_name: Target table name
            load_type: LoadType enum (FULL, INCREMENTAL, APPEND)
            schema: Database schema
            date_filter: Optional date filter for incremental
        
        Returns:
            Load result statistics
        """
        from scripts.utils.bulk_loader import BulkLoader
        
        logger.info(f"▶ TEAM 1 - T0019: Executing {load_type.value.upper()} load for {table_name}")
        
        result = {
            'table_name': table_name,
            'load_type': load_type.value,
            'source_rows': len(df),
            'loaded_rows': 0,
            'skipped_rows': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'success': False
        }
        
        try:
            if load_type == LoadType.FULL:
                # Full load: Replace entire table
                loader = BulkLoader(self.connection_string)
                stats = loader.bulk_load(df, table_name, schema=schema, if_exists='replace')
                loader.disconnect()
                
                result['loaded_rows'] = stats.get('loaded_rows', 0)
                
            elif load_type == LoadType.INCREMENTAL:
                # Incremental: Load only new records
                new_records, inc_stats = self.get_new_records(df, table_name, schema, date_filter)
                
                if not new_records.empty:
                    loader = BulkLoader(self.connection_string)
                    stats = loader.bulk_load(new_records, table_name, schema=schema, if_exists='append')
                    loader.disconnect()
                    
                    result['loaded_rows'] = stats.get('loaded_rows', 0)
                else:
                    logger.info("   No new records to load")
                    result['loaded_rows'] = 0
                
                result['skipped_rows'] = result['source_rows'] - result['loaded_rows']
                result['incremental_stats'] = inc_stats
                
            elif load_type == LoadType.APPEND:
                # Append: Load all without checking
                loader = BulkLoader(self.connection_string)
                stats = loader.bulk_load(df, table_name, schema=schema, if_exists='append')
                loader.disconnect()
                
                result['loaded_rows'] = stats.get('loaded_rows', 0)
            
            result['success'] = True
            result['end_time'] = datetime.now()
            
            # Track in history
            self.load_history.append(result.copy())
            
            logger.info(f"✅ Load complete: {result['loaded_rows']:,} rows loaded, {result['skipped_rows']:,} skipped")
            
        except Exception as e:
            result['error'] = str(e)
            result['end_time'] = datetime.now()
            logger.error(f"❌ Load failed: {e}")
        
        return result
    
    def get_last_load_date(self, table_name: str, schema: str = 'public') -> Optional[datetime]:
        """
        Get the date of the last loaded record for incremental reference
        
        Args:
            table_name: Table name
            schema: Database schema
        
        Returns:
            Maximum date in the date column, or None
        """
        config = self.TABLE_CONFIGS.get(table_name.lower(), {})
        date_column = config.get('date_column')
        
        if not date_column:
            return None
        
        engine = self.connect()
        
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT MAX("{date_column}") FROM {schema}.{table_name}'))
                max_date = result.scalar()
                
                if max_date:
                    return pd.to_datetime(max_date)
                return None
                
        except SQLAlchemyError:
            return None
    
    def get_load_history(self) -> List[Dict]:
        """Get history of load operations in this session"""
        return self.load_history.copy()
    
    def get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for a specific table"""
        return self.TABLE_CONFIGS.get(table_name.lower(), {}).copy()


# ========================================
# Convenience functions
# ========================================

def incremental_load(df: pd.DataFrame,
                     table_name: str,
                     connection_string: Optional[str] = None,
                     date_filter: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Convenience function for incremental load
    
    Args:
        df: Source DataFrame
        table_name: Target table name
        connection_string: Optional connection string
        date_filter: Optional date filter
    
    Returns:
        Load statistics
    """
    strategy = LoadStrategy(connection_string)
    try:
        return strategy.execute_load(df, table_name, LoadType.INCREMENTAL, date_filter=date_filter)
    finally:
        strategy.disconnect()


def full_load(df: pd.DataFrame,
              table_name: str,
              connection_string: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function for full load
    
    Args:
        df: Source DataFrame
        table_name: Target table name
        connection_string: Optional connection string
    
    Returns:
        Load statistics
    """
    strategy = LoadStrategy(connection_string)
    try:
        return strategy.execute_load(df, table_name, LoadType.FULL)
    finally:
        strategy.disconnect()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    sample_df = pd.DataFrame({
        'CustomerKey': [1, 2, 3, 4, 5],
        'Name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'Age': [25, 30, 35, 28, 42]
    })
    
    strategy = LoadStrategy()
    
    # Determine load type
    load_type = strategy.determine_load_type('customers')
    print(f"Load type: {load_type}")
    
    # Execute load
    result = strategy.execute_load(sample_df, 'customers_test', load_type)
    print(f"Load result: {result}")
    
    strategy.disconnect()
