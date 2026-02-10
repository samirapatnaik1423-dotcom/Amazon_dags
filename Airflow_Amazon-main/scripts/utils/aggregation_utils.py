# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Aggregation Utilities
# Tasks: T0013
# ═══════════════════════════════════════════════════════════════════════

"""
Aggregation Utilities - Data Aggregation and Grouping Operations
Part of T0013: Aggregations (groupBy, sum, min, max)

Provides:
- Multiple column grouping (single or multiple keys)
- Basic aggregations: sum, count, min, max, avg, median
- Batch processing support for large datasets
- Output to CSV and/or database
- Memory optimization options
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class AggregationEngine:
    """
    Performs data aggregations with flexible grouping and output options
    """
    
    # ========================================
    # Team 1 - T0013: Aggregations (groupBy, sum, min, max)
    # ========================================
    @staticmethod
    def aggregate(df: pd.DataFrame,
                  group_by: Union[str, List[str]],
                  agg_config: Dict[str, Union[str, List[str]]],
                  batch_size: Optional[int] = None) -> pd.DataFrame:
        """
        Perform aggregation on DataFrame
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by (string or list of strings)
            agg_config: Dict mapping column names to aggregation function(s)
                       e.g., {'Quantity': ['sum', 'count'], 'Price': 'avg'}
            batch_size: Optional batch size for memory optimization
        
        Returns:
            Aggregated DataFrame
        
        Example:
            >>> agg_config = {
            ...     'Quantity': ['sum', 'count', 'mean'],
            ...     'Price': ['min', 'max', 'median']
            ... }
            >>> result = AggregationEngine.aggregate(df, 'CustomerKey', agg_config)
        """
        try:
            if batch_size:
                logger.info(f"Processing {len(df)} rows in batches of {batch_size}")
                return AggregationEngine._aggregate_batched(df, group_by, agg_config, batch_size)
            
            # Standard aggregation
            logger.info(f"Aggregating by {group_by} with {len(agg_config)} metrics")
            
            grouped = df.groupby(group_by, as_index=False)
            result = grouped.agg(agg_config)
            
            # Flatten column names if multi-level
            if isinstance(result.columns, pd.MultiIndex):
                result.columns = ['_'.join(col).strip('_') for col in result.columns.values]
            
            logger.info(f"✅ Aggregation complete: {len(df)} → {len(result)} rows")
            return result
            
        except Exception as e:
            logger.error(f"❌ Aggregation failed: {e}")
            raise
    
    @staticmethod
    def _aggregate_batched(df: pd.DataFrame,
                          group_by: Union[str, List[str]],
                          agg_config: Dict[str, Union[str, List[str]]],
                          batch_size: int) -> pd.DataFrame:
        """
        Aggregate data in batches for memory efficiency
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by
            agg_config: Aggregation configuration
            batch_size: Batch size
        
        Returns:
            Aggregated DataFrame
        """
        results = []
        total_batches = (len(df) + batch_size - 1) // batch_size
        
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_result = batch.groupby(group_by, as_index=False).agg(agg_config)
            results.append(batch_result)
            logger.info(f"  Batch {len(results)}/{total_batches} processed")
        
        # Combine all batches
        combined = pd.concat(results, ignore_index=True)
        
        # Re-aggregate to get final results
        final_result = combined.groupby(group_by, as_index=False).agg(agg_config)
        
        # Flatten column names
        if isinstance(final_result.columns, pd.MultiIndex):
            final_result.columns = ['_'.join(col).strip('_') for col in final_result.columns.values]
        
        return final_result
    
    @staticmethod
    def group_and_summarize(df: pd.DataFrame,
                           group_by: Union[str, List[str]],
                           summarize_cols: Optional[List[str]] = None,
                           agg_funcs: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Group by columns and apply all aggregation functions
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by
            summarize_cols: Columns to summarize (default: all numeric)
            agg_funcs: Aggregation functions (default: ['sum', 'count', 'min', 'max', 'mean', 'median'])
        
        Returns:
            Summary DataFrame with all aggregations
        """
        if agg_funcs is None:
            agg_funcs = ['sum', 'count', 'min', 'max', 'mean', 'median']
        
        if summarize_cols is None:
            # Auto-detect numeric columns
            summarize_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            # Remove group_by columns from summarize list
            if isinstance(group_by, str):
                if group_by in summarize_cols:
                    summarize_cols.remove(group_by)
            else:
                for col in group_by:
                    if col in summarize_cols:
                        summarize_cols.remove(col)
        
        logger.info(f"Summarizing {len(summarize_cols)} columns by {group_by}")
        
        # Build aggregation config
        agg_config = {col: agg_funcs for col in summarize_cols}
        
        return AggregationEngine.aggregate(df, group_by, agg_config)
    
    @staticmethod
    def pivot_summary(df: pd.DataFrame,
                     index: Union[str, List[str]],
                     columns: str,
                     values: str,
                     aggfunc: str = 'sum') -> pd.DataFrame:
        """
        Create pivot table summary
        
        Args:
            df: Input DataFrame
            index: Column(s) for pivot index
            columns: Column for pivot columns
            values: Column to aggregate
            aggfunc: Aggregation function
        
        Returns:
            Pivot table DataFrame
        """
        logger.info(f"Creating pivot: index={index}, columns={columns}, values={values}")
        
        pivot = pd.pivot_table(
            df,
            index=index,
            columns=columns,
            values=values,
            aggfunc=aggfunc,
            fill_value=0
        )
        
        # Reset index to make it a regular DataFrame
        pivot = pivot.reset_index()
        
        logger.info(f"✅ Pivot table created: {pivot.shape}")
        return pivot
    
    @staticmethod
    def time_series_aggregate(df: pd.DataFrame,
                             date_column: str,
                             group_by: Optional[Union[str, List[str]]] = None,
                             freq: str = 'D',
                             agg_config: Optional[Dict[str, Union[str, List[str]]]] = None) -> pd.DataFrame:
        """
        Aggregate time series data
        
        Args:
            df: Input DataFrame
            date_column: Date column name
            group_by: Additional grouping columns
            freq: Frequency ('D'=daily, 'W'=weekly, 'M'=monthly, 'Q'=quarterly, 'Y'=yearly)
            agg_config: Aggregation configuration
        
        Returns:
            Time-aggregated DataFrame
        """
        df_copy = df.copy()
        
        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df_copy[date_column]):
            df_copy[date_column] = pd.to_datetime(df_copy[date_column])
        
        # Create time period column
        period_col = f"{date_column}_period"
        df_copy[period_col] = df_copy[date_column].dt.to_period(freq)
        
        # Build grouping columns
        if group_by:
            if isinstance(group_by, str):
                grouping = [period_col, group_by]
            else:
                grouping = [period_col] + group_by
        else:
            grouping = [period_col]
        
        # Default aggregation if not provided
        if agg_config is None:
            numeric_cols = df_copy.select_dtypes(include=[np.number]).columns.tolist()
            agg_config = {col: 'sum' for col in numeric_cols}
        
        logger.info(f"Time series aggregation: freq={freq}, grouping={grouping}")
        
        result = AggregationEngine.aggregate(df_copy, grouping, agg_config)
        
        # Convert period back to timestamp
        result[period_col] = result[period_col].dt.to_timestamp()
        
        return result
    
    @staticmethod
    def save_aggregation(df: pd.DataFrame,
                        output_path: Optional[str] = None,
                        db_manager: Optional[Any] = None,
                        table_name: Optional[str] = None,
                        save_to_csv: bool = True,
                        save_to_db: bool = False) -> Dict[str, bool]:
        """
        Save aggregation results to CSV and/or database
        
        Args:
            df: Aggregated DataFrame
            output_path: CSV output path
            db_manager: DatabaseManager instance
            table_name: Database table name
            save_to_csv: Save to CSV file
            save_to_db: Save to database
        
        Returns:
            Dict with save status {'csv': bool, 'db': bool}
        """
        result = {'csv': False, 'db': False}
        
        # Save to CSV
        if save_to_csv and output_path:
            try:
                # Create directory if needed
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_path, index=False, encoding='utf-8')
                logger.info(f"✅ Saved to CSV: {output_path}")
                result['csv'] = True
            except Exception as e:
                logger.error(f"❌ Failed to save CSV: {e}")
        
        # Save to database
        if save_to_db and db_manager and table_name:
            try:
                # Use db_manager to insert data
                db_manager.insert_dataframe(df, table_name)
                logger.info(f"✅ Saved to database table: {table_name}")
                result['db'] = True
            except Exception as e:
                logger.error(f"❌ Failed to save to database: {e}")
        
        return result
    
    @staticmethod
    def get_aggregation_stats(df: pd.DataFrame,
                             group_by: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Get statistics about aggregation
        
        Args:
            df: Input DataFrame
            group_by: Grouping columns
        
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_rows': len(df),
            'group_by': group_by if isinstance(group_by, list) else [group_by],
            'numeric_columns': df.select_dtypes(include=[np.number]).columns.tolist(),
            'unique_groups': None
        }
        
        # Count unique groups
        if isinstance(group_by, str):
            stats['unique_groups'] = df[group_by].nunique()
        else:
            stats['unique_groups'] = df.groupby(group_by).ngroups
        
        logger.info(f"Aggregation stats: {stats['total_rows']} rows → {stats['unique_groups']} groups")
        
        return stats


# Convenience functions
def aggregate_by_customer(df: pd.DataFrame,
                         customer_col: str = 'CustomerKey') -> pd.DataFrame:
    """
    Quick aggregation by customer
    
    Args:
        df: Sales/transaction DataFrame
        customer_col: Customer key column name
    
    Returns:
        Customer-level aggregated DataFrame
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if customer_col in numeric_cols:
        numeric_cols.remove(customer_col)
    
    agg_config = {col: ['sum', 'count', 'mean'] for col in numeric_cols}
    
    return AggregationEngine.aggregate(df, customer_col, agg_config)


def aggregate_by_product(df: pd.DataFrame,
                        product_col: str = 'ProductKey') -> pd.DataFrame:
    """
    Quick aggregation by product
    
    Args:
        df: Sales/transaction DataFrame
        product_col: Product key column name
    
    Returns:
        Product-level aggregated DataFrame
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if product_col in numeric_cols:
        numeric_cols.remove(product_col)
    
    agg_config = {col: ['sum', 'count', 'mean', 'min', 'max'] for col in numeric_cols}
    
    return AggregationEngine.aggregate(df, product_col, agg_config)


def aggregate_daily_sales(df: pd.DataFrame,
                         date_col: str = 'Order Date') -> pd.DataFrame:
    """
    Quick daily sales aggregation
    
    Args:
        df: Sales DataFrame
        date_col: Date column name
    
    Returns:
        Daily aggregated DataFrame
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    agg_config = {col: 'sum' for col in numeric_cols}
    
    return AggregationEngine.time_series_aggregate(
        df,
        date_column=date_col,
        freq='D',
        agg_config=agg_config
    )
