"""
SQL Transformation Engine
TEAM 3 - SPRINT 2-3 (PHASE 1)
Execute SQL transformations using templates and custom queries
"""

import logging
from typing import Dict, List, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from pathlib import Path
from datetime import datetime

from .templates import SQLTemplates

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SQLTransformationEngine:
    """
    Execute SQL transformations using templates or custom queries
    
    Features:
    - Execute templated SQL transformations
    - Custom SQL query execution
    - Results export (CSV, database)
    - Transaction management
    - Query history tracking
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize SQL transformation engine
        
        Args:
            connection_string: SQLAlchemy connection string
        """
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
        self.templates = SQLTemplates()
        self.query_history: List[Dict[str, Any]] = []
        
        self._connect()
        logger.info("✅ SQLTransformationEngine initialized")
    
    def _connect(self) -> None:
        """Establish database connection"""
        try:
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection established")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def execute_template(self, template_name: str, **kwargs) -> pd.DataFrame:
        """
        Execute a SQL template transformation
        
        Args:
            template_name: Name of template method (e.g., 'group_by_aggregate')
            **kwargs: Template parameters
            
        Returns:
            DataFrame with query results
        """
        logger.info(f"🔄 Executing template: {template_name}")
        
        try:
            # Get template method
            template_method = getattr(self.templates, template_name)
            
            # Generate SQL query
            query = template_method(**kwargs)
            
            # Execute query
            result = self.execute_query(query)
            
            logger.info(f"✅ Template executed: {len(result)} records returned")
            return result
            
        except AttributeError:
            logger.error(f"❌ Template not found: {template_name}")
            raise ValueError(f"Template '{template_name}' does not exist")
        except Exception as e:
            logger.error(f"❌ Template execution failed: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Execute custom SQL query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            DataFrame with query results
        """
        if not self.engine:
            raise RuntimeError("Not connected to database")
        
        start_time = datetime.now()
        logger.info(f"🔍 Executing query: {query[:100]}...")
        
        try:
            if params:
                df = pd.read_sql_query(text(query), self.engine, params=params)
            else:
                df = pd.read_sql_query(query, self.engine)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Log to history
            self.query_history.append({
                'timestamp': start_time.isoformat(),
                'query': query[:200],
                'rows_returned': len(df),
                'execution_time_seconds': execution_time,
                'status': 'success'
            })
            
            logger.info(f"✅ Query executed: {len(df)} rows in {execution_time:.2f}s")
            return df
            
        except Exception as e:
            self.query_history.append({
                'timestamp': start_time.isoformat(),
                'query': query[:200],
                'status': 'failed',
                'error': str(e)
            })
            logger.error(f"❌ Query execution failed: {e}")
            raise
    
    def execute_transformation(self, transformation_type: str, **params) -> pd.DataFrame:
        """
        Execute common transformations with simplified interface
        
        Args:
            transformation_type: Type of transformation
            **params: Transformation parameters
            
        Returns:
            DataFrame with results
        """
        transformations = {
            'aggregate': 'group_by_aggregate',
            'rank': 'row_number_ranking',
            'running_total': 'running_total',
            'deduplicate': 'deduplicate_with_priority',
            'incremental': 'incremental_extract',
            'date_aggregate': 'date_range_aggregation'
        }
        
        template_name = transformations.get(transformation_type)
        if not template_name:
            raise ValueError(f"Unknown transformation type: {transformation_type}")
        
        return self.execute_template(template_name, **params)
    
    def save_to_table(self, df: pd.DataFrame, table_name: str, 
                     schema: str = None, if_exists: str = 'replace') -> None:
        """
        Save DataFrame to database table
        
        Args:
            df: DataFrame to save
            table_name: Target table name
            schema: Schema name
            if_exists: 'replace', 'append', or 'fail'
        """
        if not self.engine:
            raise RuntimeError("Not connected to database")
        
        full_table = f"{schema}.{table_name}" if schema else table_name
        logger.info(f"💾 Saving {len(df)} rows to {full_table}")
        
        try:
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False
            )
            logger.info(f"✅ Successfully saved to {full_table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save to table: {e}")
            raise
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save DataFrame to CSV
        
        Args:
            df: DataFrame to save
            output_path: Output CSV path
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Saving to CSV: {output_path}")
        df.to_csv(output_path, index=False)
        logger.info(f"✅ Successfully saved {len(df)} records")
    
    def get_query_history(self) -> pd.DataFrame:
        """
        Get query execution history
        
        Returns:
            DataFrame with query history
        """
        if not self.query_history:
            logger.warning("⚠️ No query history available")
            return pd.DataFrame()
        
        return pd.DataFrame(self.query_history)
    
    def close(self) -> None:
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("🔌 Database connection closed")


# ═══════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Initialize engine
    connection_string = "postgresql+psycopg2://airflow:airflow@localhost:5434/airflow"
    engine = SQLTransformationEngine(connection_string)
    
    # Example 1: Aggregate transformation
    result = engine.execute_transformation(
        'aggregate',
        table='sales',
        group_columns=['store_key'],
        agg_columns={'quantity': 'SUM', 'order_number': 'COUNT'},
        schema='etl_output'
    )
    print("Aggregation Result:")
    print(result.head())
    
    # Example 2: Custom query
    custom_query = """
    SELECT 
        store_key, 
        COUNT(*) as order_count,
        SUM(quantity) as total_quantity
    FROM etl_output.sales
    GROUP BY store_key
    LIMIT 10
    """
    result2 = engine.execute_query(custom_query)
    print("\nCustom Query Result:")
    print(result2)
    
    # Get query history
    history = engine.get_query_history()
    print(f"\nQuery History: {len(history)} queries executed")
    
    engine.close()
