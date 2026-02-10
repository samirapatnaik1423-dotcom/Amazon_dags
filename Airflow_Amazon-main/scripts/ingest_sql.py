"""
SQL Database Ingestion Module
TEAM 2 - SPRINT 1 (PHASE 1)
Handles extraction from SQL databases (PostgreSQL, MySQL, SQL Server)
"""

import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SQLIngester:
    """
    Handles SQL database ingestion with support for:
    - PostgreSQL, MySQL, SQL Server, Oracle
    - Connection pooling
    - Query execution
    - Table extraction
    - Incremental extraction
    """
    
    SUPPORTED_DATABASES = {
        'postgresql': 'postgresql+psycopg2',
        'mysql': 'mysql+pymysql',
        'sqlserver': 'mssql+pyodbc',
        'oracle': 'oracle+cx_oracle'
    }
    
    def __init__(self, connection_string: Optional[str] = None, **kwargs):
        """
        Initialize SQL ingester
        
        Args:
            connection_string: SQLAlchemy connection string
            **kwargs: Connection parameters (host, port, database, user, password)
        """
        self.engine: Optional[Engine] = None
        
        if connection_string:
            self.connect(connection_string)
        elif kwargs:
            self.connect_from_params(**kwargs)
        
        logger.info("✅ SQLIngester initialized")
    
    def connect(self, connection_string: str) -> None:
        """
        Connect to database using connection string
        
        Args:
            connection_string: SQLAlchemy connection string
        """
        logger.info(f"🔌 Connecting to database: {connection_string.split('@')[1] if '@' in connection_string else 'local'}")
        
        try:
            self.engine = create_engine(connection_string, pool_pre_ping=True)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection successful")
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def connect_from_params(self, db_type: str, host: str, port: int, 
                           database: str, user: str, password: str,
                           **extra_params) -> None:
        """
        Connect to database using parameters
        
        Args:
            db_type: Database type ('postgresql', 'mysql', 'sqlserver', 'oracle')
            host: Database host
            port: Database port
            database: Database name
            user: Username
            password: Password
            **extra_params: Additional connection parameters
        """
        if db_type not in self.SUPPORTED_DATABASES:
            raise ValueError(f"Unsupported database type: {db_type}. Supported: {list(self.SUPPORTED_DATABASES.keys())}")
        
        dialect = self.SUPPORTED_DATABASES[db_type]
        connection_string = f"{dialect}://{user}:{password}@{host}:{port}/{database}"
        
        # Add extra parameters
        if extra_params:
            params = '&'.join([f"{k}={v}" for k, v in extra_params.items()])
            connection_string += f"?{params}"
        
        self.connect(connection_string)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            query: SQL query
            params: Query parameters (for parameterized queries)
            
        Returns:
            DataFrame with query results
        """
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        logger.info(f"🔍 Executing query: {query[:100]}...")
        
        try:
            if params:
                df = pd.read_sql_query(text(query), self.engine, params=params)
            else:
                df = pd.read_sql_query(query, self.engine)
            
            logger.info(f"✅ Query executed successfully: {len(df)} records retrieved")
            return df
            
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            raise
    
    def extract_table(self, table_name: str, schema: Optional[str] = None, 
                     columns: Optional[List[str]] = None,
                     where_clause: Optional[str] = None,
                     limit: Optional[int] = None) -> pd.DataFrame:
        """
        Extract entire table or specific columns
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
            columns: List of columns to extract (None = all columns)
            where_clause: WHERE clause filter
            limit: Maximum number of records
            
        Returns:
            DataFrame with table data
        """
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        full_table = f"{schema}.{table_name}" if schema else table_name
        logger.info(f"📊 Extracting table: {full_table}")
        
        try:
            # Build query
            cols = ", ".join(columns) if columns else "*"
            query = f"SELECT {cols} FROM {full_table}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql_query(query, self.engine)
            logger.info(f"✅ Extracted {len(df)} records from {full_table}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Table extraction failed: {e}")
            raise
    
    def extract_incremental(self, table_name: str, timestamp_column: str,
                           last_extracted: str, schema: Optional[str] = None) -> pd.DataFrame:
        """
        Extract new/updated records since last extraction
        
        Args:
            table_name: Table name
            timestamp_column: Column to use for incremental extraction
            last_extracted: Last extraction timestamp (ISO format)
            schema: Schema name (optional)
            
        Returns:
            DataFrame with new records
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        logger.info(f"🔄 Incremental extraction from {full_table} since {last_extracted}")
        
        where_clause = f"{timestamp_column} > '{last_extracted}'"
        df = self.extract_table(table_name, schema=schema, where_clause=where_clause)
        
        logger.info(f"✅ Found {len(df)} new/updated records")
        return df
    
    def get_table_metadata(self, table_name: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """
        Get metadata about a table
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
            
        Returns:
            Metadata dictionary
        """
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        full_table = f"{schema}.{table_name}" if schema else table_name
        logger.info(f"📋 Getting metadata for {full_table}")
        
        try:
            inspector = inspect(self.engine)
            
            # Get columns
            columns = inspector.get_columns(table_name, schema=schema)
            
            # Get primary keys
            pk = inspector.get_pk_constraint(table_name, schema=schema)
            
            # Get row count
            query = f"SELECT COUNT(*) as count FROM {full_table}"
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                row_count = result.fetchone()[0]
            
            metadata = {
                'table_name': table_name,
                'schema': schema,
                'column_count': len(columns),
                'columns': [{'name': col['name'], 'type': str(col['type'])} for col in columns],
                'primary_keys': pk.get('constrained_columns', []),
                'row_count': row_count,
                'retrieved_at': datetime.now().isoformat()
            }
            
            logger.info(f"✅ Metadata retrieved: {row_count} rows, {len(columns)} columns")
            return metadata
            
        except Exception as e:
            logger.error(f"❌ Failed to get table metadata: {e}")
            raise
    
    def list_tables(self, schema: Optional[str] = None) -> List[str]:
        """
        List all tables in database or schema
        
        Args:
            schema: Schema name (None = all schemas)
            
        Returns:
            List of table names
        """
        if not self.engine:
            raise RuntimeError("Not connected to database. Call connect() first.")
        
        logger.info(f"📋 Listing tables in {'schema: ' + schema if schema else 'all schemas'}")
        
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=schema)
            logger.info(f"✅ Found {len(tables)} tables")
            return tables
            
        except Exception as e:
            logger.error(f"❌ Failed to list tables: {e}")
            raise
    
    def save_to_csv(self, df: pd.DataFrame, output_path: Union[str, Path]) -> None:
        """
        Save extracted data to CSV
        
        Args:
            df: DataFrame to save
            output_path: Output CSV path
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Saving to CSV: {output_path}")
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved {len(df)} records")
    
    def test_connection(self) -> bool:
        """
        Test database connection
        
        Returns:
            True if connection successful, False otherwise
        """
        if not self.engine:
            logger.error("❌ No connection to test")
            return False
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Connection test passed")
            return True
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False
    
    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("🔌 Database connection closed")


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Example 1: Connect using connection string
    connection_string = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"
    ingester = SQLIngester(connection_string=connection_string)
    
    # Test connection
    ingester.test_connection()
    
    # List tables
    tables = ingester.list_tables(schema='etl_output')
    print(f"\nTables in etl_output schema: {tables}")
    
    # Example 2: Extract entire table
    if tables:
        df = ingester.extract_table(tables[0], schema='etl_output', limit=10)
        print(f"\nSample data from {tables[0]}:")
        print(df.head())
        
        # Get metadata
        metadata = ingester.get_table_metadata(tables[0], schema='etl_output')
        print(f"\nTable metadata: {metadata}")
    
    # Example 3: Custom query
    query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'etl_output'"
    df = ingester.execute_query(query)
    print(f"\nETL tables: {len(df)}")
    print(df)
    
    ingester.close()
