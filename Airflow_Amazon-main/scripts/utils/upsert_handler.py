# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Upsert Handler
# Tasks: T0021
# ═══════════════════════════════════════════════════════════════════════

"""
Upsert Handler - Insert or Update records based on primary key
Part of T0021: Build upsert (insert/update) logic

Provides:
- Insert new records (primary key doesn't exist)
- Update existing records (primary key exists)
- Batch upsert for performance
- Conflict resolution strategies
- PostgreSQL ON CONFLICT support
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Union
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class UpsertMode(str):
    """Upsert conflict resolution modes"""
    UPDATE = "update"       # Update all columns on conflict
    IGNORE = "ignore"       # Skip conflicting records
    REPLACE = "replace"     # Delete and re-insert


class UpsertHandler:
    """
    TEAM 1 - T0021: Upsert (Insert/Update) logic handler
    
    Handles:
    - New records: INSERT
    - Existing records: UPDATE
    
    Uses PostgreSQL's ON CONFLICT clause for atomic upserts.
    
    Tables:
    - Customers: Upsert by CustomerKey
    - Sales: Upsert by Order Number
    - Products: Upsert by ProductKey
    - Stores: Upsert by StoreKey
    - Exchange_Rates: Upsert by Date + Currency
    """
    
    # Default Airflow PostgreSQL connection
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    # Primary key configurations
    TABLE_PRIMARY_KEYS = {
        'customers': ['CustomerKey'],
        'sales': ['Order Number'],
        'products': ['ProductKey'],
        'stores': ['StoreKey'],
        'exchange_rates': ['Date', 'Currency']  # Composite key
    }
    
    def __init__(self,
                 connection_string: Optional[str] = None,
                 batch_size: int = 5000,
                 mode: str = UpsertMode.UPDATE):
        """
        Initialize upsert handler
        
        Args:
            connection_string: Database connection string
            batch_size: Number of records per batch upsert
            mode: Conflict resolution mode (update, ignore, replace)
        """
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.batch_size = batch_size
        self.mode = mode
        self.engine: Optional[Engine] = None
        self.stats = {
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0
        }
        
        logger.info(f"▶ TEAM 1 - T0021: UpsertHandler initialized (mode={mode}, batch_size={batch_size})")
    
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
    # Team 1 - T0021: Upsert Logic (Insert/Update)
    # ========================================
    def upsert(self,
               df: pd.DataFrame,
               table_name: str,
               schema: str = 'public',
               primary_keys: Optional[List[str]] = None,
               update_columns: Optional[List[str]] = None,
               mode: Optional[str] = None) -> Dict[str, Any]:
        """
        Perform upsert operation: INSERT new records, UPDATE existing ones
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            schema: Database schema
            primary_keys: Columns that form the primary key (auto-detected if None)
            update_columns: Columns to update on conflict (all non-key columns if None)
            mode: Override upsert mode for this operation
        
        Returns:
            Upsert statistics dictionary
        """
        logger.info(f"▶ TEAM 1 - T0021: Starting upsert to {schema}.{table_name}")
        
        mode = mode or self.mode
        
        self.stats = {
            'table_name': table_name,
            'mode': mode,
            'total_rows': len(df),
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'failed': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        if df.empty:
            logger.warning("⚠️ Empty DataFrame, nothing to upsert")
            self.stats['end_time'] = datetime.now()
            return self.stats
        
        # Auto-detect primary keys
        if primary_keys is None:
            primary_keys = self.TABLE_PRIMARY_KEYS.get(table_name.lower(), [])
        
        if not primary_keys:
            logger.error(f"❌ No primary key configured for {table_name}")
            raise ValueError(f"No primary key configured for {table_name}")
        
        # Validate primary keys exist in DataFrame
        missing_keys = [k for k in primary_keys if k not in df.columns]
        if missing_keys:
            logger.error(f"❌ Primary key columns not in DataFrame: {missing_keys}")
            raise ValueError(f"Primary key columns not in DataFrame: {missing_keys}")
        
        # Determine update columns (all except primary keys)
        if update_columns is None:
            update_columns = [c for c in df.columns if c not in primary_keys]
        
        logger.info(f"   Primary keys: {primary_keys}")
        logger.info(f"   Update columns: {len(update_columns)} columns")
        
        engine = self.connect()
        
        # Ensure table exists (create if needed)
        self._ensure_table_exists(df, table_name, schema, primary_keys)
        
        try:
            # Process in batches
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            for batch_num, start_idx in enumerate(range(0, len(df), self.batch_size), 1):
                end_idx = min(start_idx + self.batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                batch_stats = self._upsert_batch(
                    batch_df, table_name, schema, 
                    primary_keys, update_columns, mode
                )
                
                self.stats['inserted'] += batch_stats.get('inserted', 0)
                self.stats['updated'] += batch_stats.get('updated', 0)
                self.stats['skipped'] += batch_stats.get('skipped', 0)
                
                logger.info(f"   Batch {batch_num}/{total_batches}: "
                           f"+{batch_stats.get('inserted', 0)} inserted, "
                           f"~{batch_stats.get('updated', 0)} updated")
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            logger.info(f"✅ Upsert complete: {self.stats['inserted']:,} inserted, "
                       f"{self.stats['updated']:,} updated in {duration:.2f}s")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"❌ Upsert failed: {e}")
            self.stats['error'] = str(e)
            self.stats['end_time'] = datetime.now()
            raise
    
    def _upsert_batch(self,
                      df: pd.DataFrame,
                      table_name: str,
                      schema: str,
                      primary_keys: List[str],
                      update_columns: List[str],
                      mode: str) -> Dict[str, int]:
        """
        Upsert a single batch of records using PostgreSQL ON CONFLICT
        
        Returns:
            Batch statistics
        """
        engine = self.connect()
        stats = {'inserted': 0, 'updated': 0, 'skipped': 0}
        
        try:
            # Reflect the table
            metadata = MetaData()
            metadata.reflect(bind=engine, schema=schema, only=[table_name])
            table = metadata.tables.get(f'{schema}.{table_name}')
            
            if table is None:
                logger.error(f"❌ Table {schema}.{table_name} not found")
                return stats
            
            # Convert DataFrame to list of dicts
            records = df.to_dict('records')
            
            # Build upsert statement
            with engine.connect() as conn:
                for record in records:
                    try:
                        insert_stmt = pg_insert(table).values(**record)
                        
                        if mode == UpsertMode.UPDATE:
                            # ON CONFLICT DO UPDATE
                            update_dict = {c: insert_stmt.excluded[c] for c in update_columns if c in record}
                            upsert_stmt = insert_stmt.on_conflict_do_update(
                                index_elements=primary_keys,
                                set_=update_dict
                            )
                        elif mode == UpsertMode.IGNORE:
                            # ON CONFLICT DO NOTHING
                            upsert_stmt = insert_stmt.on_conflict_do_nothing(
                                index_elements=primary_keys
                            )
                        else:
                            upsert_stmt = insert_stmt
                        
                        result = conn.execute(upsert_stmt)
                        
                        # Track insert vs update (approximate)
                        if result.rowcount > 0:
                            if mode == UpsertMode.UPDATE:
                                # Can't distinguish insert from update with ON CONFLICT
                                stats['inserted'] += 1  # Count as insert
                            else:
                                stats['inserted'] += result.rowcount
                        else:
                            stats['skipped'] += 1
                            
                    except SQLAlchemyError as e:
                        logger.warning(f"   Record upsert failed: {e}")
                        stats['skipped'] += 1
                
                conn.commit()
            
            return stats
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Batch upsert failed: {e}")
            stats['failed'] = len(df)
            return stats
    
    def _ensure_table_exists(self,
                             df: pd.DataFrame,
                             table_name: str,
                             schema: str,
                             primary_keys: List[str]) -> bool:
        """
        Ensure table exists, create if not
        
        Returns:
            True if table exists or was created
        """
        engine = self.connect()
        inspector = inspect(engine)
        
        if table_name.lower() in [t.lower() for t in inspector.get_table_names(schema=schema)]:
            return True
        
        logger.info(f"   Creating table {schema}.{table_name}...")
        
        try:
            # Create table from DataFrame schema
            df.head(0).to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='replace',
                index=False
            )
            
            # Add primary key constraint
            if primary_keys:
                pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
                with engine.connect() as conn:
                    conn.execute(text(f"""
                        ALTER TABLE {schema}.{table_name}
                        ADD PRIMARY KEY ({pk_cols})
                    """))
                    conn.commit()
            
            logger.info(f"   ✅ Table created with primary key: {primary_keys}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"   ❌ Table creation failed: {e}")
            return False
    
    def upsert_smart(self,
                     df: pd.DataFrame,
                     table_name: str,
                     schema: str = 'public') -> Dict[str, Any]:
        """
        Smart upsert: Separates new and existing records for efficient processing
        
        Checks which records exist before upserting for better statistics.
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            schema: Database schema
        
        Returns:
            Detailed upsert statistics
        """
        logger.info(f"▶ TEAM 1 - T0021: Smart upsert to {schema}.{table_name}")
        
        primary_keys = self.TABLE_PRIMARY_KEYS.get(table_name.lower(), [])
        if not primary_keys:
            return self.upsert(df, table_name, schema)
        
        engine = self.connect()
        
        # Check which records exist
        inspector = inspect(engine)
        if table_name.lower() not in [t.lower() for t in inspector.get_table_names(schema=schema)]:
            # Table doesn't exist, all are inserts
            return self.upsert(df, table_name, schema)
        
        try:
            # Get existing keys
            pk_col = primary_keys[0]  # Use first key for simple lookup
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT "{pk_col}" FROM {schema}.{table_name}'))
                existing_keys = set(row[0] for row in result)
            
            # Split DataFrame
            new_records = df[~df[pk_col].isin(existing_keys)]
            existing_records = df[df[pk_col].isin(existing_keys)]
            
            logger.info(f"   New records: {len(new_records):,}, Existing: {len(existing_records):,}")
            
            stats = {
                'table_name': table_name,
                'total_rows': len(df),
                'new_records': len(new_records),
                'existing_records': len(existing_records),
                'inserted': 0,
                'updated': 0,
                'start_time': datetime.now()
            }
            
            # Insert new records
            if not new_records.empty:
                new_records.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                stats['inserted'] = len(new_records)
                logger.info(f"   Inserted {len(new_records):,} new records")
            
            # Update existing records
            if not existing_records.empty:
                update_stats = self.upsert(existing_records, table_name, schema, 
                                          primary_keys, mode=UpsertMode.UPDATE)
                stats['updated'] = update_stats.get('inserted', 0)  # All updates
            
            stats['end_time'] = datetime.now()
            logger.info(f"✅ Smart upsert complete: {stats['inserted']:,} inserted, {stats['updated']:,} updated")
            
            return stats
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Smart upsert failed: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get upsert statistics"""
        return self.stats.copy()


# ========================================
# Convenience functions
# ========================================

def quick_upsert(df: pd.DataFrame,
                 table_name: str,
                 connection_string: Optional[str] = None) -> Dict[str, Any]:
    """
    Quick upsert without creating UpsertHandler instance
    
    Args:
        df: DataFrame to upsert
        table_name: Target table name
        connection_string: Optional connection string
    
    Returns:
        Upsert statistics
    """
    handler = UpsertHandler(connection_string)
    try:
        return handler.upsert_smart(df, table_name)
    finally:
        handler.disconnect()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    sample_df = pd.DataFrame({
        'CustomerKey': [1, 2, 3],
        'Name': ['Alice Updated', 'Bob Updated', 'New Charlie'],
        'Age': [26, 31, 35]
    })
    
    handler = UpsertHandler()
    stats = handler.upsert_smart(sample_df, 'customers_test')
    print(f"\nUpsert stats: {stats}")
    handler.disconnect()
