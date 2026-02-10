# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 4: Bulk Loader Utility
# Tasks: T0018
# ═══════════════════════════════════════════════════════════════════════

"""
Bulk Load Utility - High-performance batch database loading
Part of T0018: Implement bulk load utility

Provides:
- Bulk insert with configurable batch sizes
- Memory-efficient chunked processing
- Progress tracking and logging
- Transaction management
- Connection pooling support
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple, Callable
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class BulkLoader:
    """
    TEAM 1 - T0018: High-performance bulk data loader
    
    Features:
    - Batch processing with configurable chunk size
    - Memory-efficient loading for large datasets
    - Progress callbacks for monitoring
    - Automatic table creation from DataFrame schema
    - Transaction management (commit per batch or all-at-once)
    """
    
    # Default Airflow PostgreSQL connection
    DEFAULT_CONNECTION = "postgresql://airflow:airflow@localhost:5432/airflow"
    
    def __init__(self, 
                 connection_string: Optional[str] = None,
                 batch_size: int = 10000,
                 use_transactions: bool = True):
        """
        Initialize bulk loader
        
        Args:
            connection_string: Database connection string
            batch_size: Number of rows per batch insert
            use_transactions: Whether to use transaction management
        """
        self.connection_string = connection_string or self.DEFAULT_CONNECTION
        self.batch_size = batch_size
        self.use_transactions = use_transactions
        self.engine: Optional[Engine] = None
        self.stats = {
            'total_rows': 0,
            'loaded_rows': 0,
            'failed_rows': 0,
            'batches_processed': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info(f"▶ TEAM 1 - T0018: BulkLoader initialized (batch_size={batch_size})")
    
    def connect(self) -> Engine:
        """
        Establish database connection
        
        Returns:
            SQLAlchemy Engine
        """
        if self.engine is None:
            try:
                self.engine = create_engine(
                    self.connection_string,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True
                )
                # Test connection
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("✅ Database connection established")
            except SQLAlchemyError as e:
                logger.error(f"❌ Database connection failed: {e}")
                raise
        return self.engine
    
    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("✅ Database connection closed")
    
    # ========================================
    # Team 1 - T0018: Bulk Load Operations
    # ========================================
    def bulk_load(self,
                  df: pd.DataFrame,
                  table_name: str,
                  schema: str = 'public',
                  if_exists: str = 'append',
                  create_table: bool = True,
                  dtype: Optional[Dict] = None,
                  progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        """
        Bulk load DataFrame to database table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            schema: Database schema (default: 'public')
            if_exists: How to handle existing table ('fail', 'replace', 'append')
            create_table: Whether to create table if not exists
            dtype: Optional SQLAlchemy column type mapping
            progress_callback: Optional callback(loaded_rows, total_rows)
        
        Returns:
            Load statistics dictionary
        """
        logger.info(f"▶ TEAM 1 - T0018: Starting bulk load to {schema}.{table_name}")
        logger.info(f"   Rows: {len(df):,}, Batch size: {self.batch_size:,}")
        
        self.stats = {
            'table_name': table_name,
            'schema': schema,
            'total_rows': len(df),
            'loaded_rows': 0,
            'failed_rows': 0,
            'batches_processed': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        if df.empty:
            logger.warning("⚠️ Empty DataFrame, nothing to load")
            self.stats['end_time'] = datetime.now()
            return self.stats
        
        engine = self.connect()
        
        try:
            # Calculate total batches
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            
            # Process in batches
            for batch_num, start_idx in enumerate(range(0, len(df), self.batch_size), 1):
                end_idx = min(start_idx + self.batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                try:
                    # For first batch, handle table creation/replacement
                    batch_if_exists = if_exists if batch_num == 1 else 'append'
                    
                    batch_df.to_sql(
                        name=table_name,
                        con=engine,
                        schema=schema,
                        if_exists=batch_if_exists,
                        index=False,
                        dtype=dtype,
                        method='multi'  # Use multi-row INSERT for performance
                    )
                    
                    self.stats['loaded_rows'] += len(batch_df)
                    self.stats['batches_processed'] += 1
                    
                    # Progress callback
                    if progress_callback:
                        progress_callback(self.stats['loaded_rows'], self.stats['total_rows'])
                    
                    # Log progress
                    pct = (self.stats['loaded_rows'] / self.stats['total_rows']) * 100
                    logger.info(f"   Batch {batch_num}/{total_batches}: {len(batch_df):,} rows ({pct:.1f}%)")
                    
                except SQLAlchemyError as e:
                    self.stats['failed_rows'] += len(batch_df)
                    logger.error(f"❌ Batch {batch_num} failed: {e}")
                    # Continue with next batch (Option A: Log, skip, continue)
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            rows_per_sec = self.stats['loaded_rows'] / duration if duration > 0 else 0
            
            logger.info(f"✅ Bulk load complete: {self.stats['loaded_rows']:,}/{self.stats['total_rows']:,} rows")
            logger.info(f"   Duration: {duration:.2f}s, Rate: {rows_per_sec:,.0f} rows/sec")
            
            if self.stats['failed_rows'] > 0:
                logger.warning(f"⚠️ Failed rows: {self.stats['failed_rows']:,}")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"❌ Bulk load failed: {e}")
            self.stats['end_time'] = datetime.now()
            self.stats['error'] = str(e)
            raise
    
    def bulk_load_from_csv(self,
                           csv_path: str,
                           table_name: str,
                           schema: str = 'public',
                           if_exists: str = 'append',
                           chunksize: Optional[int] = None,
                           **read_csv_kwargs) -> Dict[str, Any]:
        """
        Bulk load directly from CSV file (memory-efficient for large files)
        
        Args:
            csv_path: Path to CSV file
            table_name: Target table name
            schema: Database schema
            if_exists: How to handle existing table
            chunksize: Rows to read per chunk (default: batch_size)
            **read_csv_kwargs: Additional pandas read_csv arguments
        
        Returns:
            Load statistics dictionary
        """
        logger.info(f"▶ TEAM 1 - T0018: Loading CSV to {schema}.{table_name}")
        logger.info(f"   Source: {csv_path}")
        
        chunksize = chunksize or self.batch_size
        engine = self.connect()
        
        self.stats = {
            'table_name': table_name,
            'schema': schema,
            'source_file': csv_path,
            'total_rows': 0,
            'loaded_rows': 0,
            'failed_rows': 0,
            'batches_processed': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        try:
            # Read and load in chunks
            chunk_num = 0
            for chunk_df in pd.read_csv(csv_path, chunksize=chunksize, **read_csv_kwargs):
                chunk_num += 1
                self.stats['total_rows'] += len(chunk_df)
                
                try:
                    batch_if_exists = if_exists if chunk_num == 1 else 'append'
                    
                    chunk_df.to_sql(
                        name=table_name,
                        con=engine,
                        schema=schema,
                        if_exists=batch_if_exists,
                        index=False,
                        method='multi'
                    )
                    
                    self.stats['loaded_rows'] += len(chunk_df)
                    self.stats['batches_processed'] += 1
                    
                    logger.info(f"   Chunk {chunk_num}: {len(chunk_df):,} rows loaded")
                    
                except SQLAlchemyError as e:
                    self.stats['failed_rows'] += len(chunk_df)
                    logger.error(f"❌ Chunk {chunk_num} failed: {e}")
            
            self.stats['end_time'] = datetime.now()
            duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            logger.info(f"✅ CSV bulk load complete: {self.stats['loaded_rows']:,} rows in {duration:.2f}s")
            
            return self.stats
            
        except Exception as e:
            logger.error(f"❌ CSV bulk load failed: {e}")
            self.stats['end_time'] = datetime.now()
            self.stats['error'] = str(e)
            raise
    
    def create_table_from_df(self,
                             df: pd.DataFrame,
                             table_name: str,
                             schema: str = 'public',
                             primary_key: Optional[str] = None,
                             dtype: Optional[Dict] = None) -> bool:
        """
        Create database table from DataFrame schema
        
        Args:
            df: DataFrame to derive schema from
            table_name: Target table name
            schema: Database schema
            primary_key: Column to set as primary key
            dtype: Optional SQLAlchemy type mapping
        
        Returns:
            True if table created successfully
        """
        logger.info(f"▶ Creating table {schema}.{table_name} from DataFrame schema")
        
        engine = self.connect()
        
        try:
            # Create table with first 0 rows to define schema
            df.head(0).to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists='replace',
                index=False,
                dtype=dtype
            )
            
            # Add primary key constraint if specified
            if primary_key and primary_key in df.columns:
                with engine.connect() as conn:
                    conn.execute(text(f"""
                        ALTER TABLE {schema}.{table_name}
                        ADD PRIMARY KEY ({primary_key})
                    """))
                    conn.commit()
                logger.info(f"   Added primary key: {primary_key}")
            
            logger.info(f"✅ Table {schema}.{table_name} created")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"❌ Table creation failed: {e}")
            return False
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """Check if table exists in database"""
        engine = self.connect()
        inspector = inspect(engine)
        return table_name in inspector.get_table_names(schema=schema)
    
    def get_row_count(self, table_name: str, schema: str = 'public') -> int:
        """Get current row count of table"""
        engine = self.connect()
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
                return result.scalar()
        except SQLAlchemyError:
            return 0
    
    def truncate_table(self, table_name: str, schema: str = 'public') -> bool:
        """Truncate (empty) a table"""
        engine = self.connect()
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                conn.commit()
            logger.info(f"✅ Table {schema}.{table_name} truncated")
            return True
        except SQLAlchemyError as e:
            logger.error(f"❌ Truncate failed: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get loading statistics"""
        return self.stats.copy()


# ========================================
# Convenience functions
# ========================================

def quick_bulk_load(df: pd.DataFrame,
                    table_name: str,
                    connection_string: Optional[str] = None,
                    if_exists: str = 'replace') -> Dict[str, Any]:
    """
    Quick bulk load without creating BulkLoader instance
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        connection_string: Optional connection string
        if_exists: How to handle existing table
    
    Returns:
        Load statistics
    """
    loader = BulkLoader(connection_string=connection_string)
    try:
        return loader.bulk_load(df, table_name, if_exists=if_exists)
    finally:
        loader.disconnect()


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Create sample data
    sample_df = pd.DataFrame({
        'id': range(1, 1001),
        'name': [f'Item_{i}' for i in range(1, 1001)],
        'value': np.random.randn(1000)
    })
    
    # Bulk load
    loader = BulkLoader(batch_size=500)
    stats = loader.bulk_load(sample_df, 'test_bulk_load', if_exists='replace')
    print(f"\nLoad stats: {stats}")
    loader.disconnect()
