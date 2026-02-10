# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: DateTime Utilities
# Tasks: T0016
# ═══════════════════════════════════════════════════════════════════════

"""
DateTime Transformation Utilities - Date feature extraction and intervals
Part of T0016: Date/Time Transformations

Provides:
- Basic date parts: year, month, day, day_of_week, quarter
- Intervals: days_between order and delivery
- Customer tenure: days since first purchase
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DateTimeEngine:
    """
    Date/time feature extraction and interval calculations
    """
    
    # ========================================
    # Team 1 - T0016: Date/Time Transformations (date parts)
    # ========================================
    @staticmethod
    def extract_date_parts(df: pd.DataFrame,
                          date_col: str,
                          prefix: Optional[str] = None) -> pd.DataFrame:
        """
        Extract basic date parts
        
        Features:
        - year, month, day, day_of_week, quarter
        
        Args:
            df: Input DataFrame
            date_col: Date column name
            prefix: Optional prefix for feature names
        
        Returns:
            DataFrame with new date part columns
        """
        df_new = df.copy()
        
        if prefix is None:
            prefix = date_col.lower().replace(' ', '_')
        
        # Ensure datetime
        if not pd.api.types.is_datetime64_any_dtype(df_new[date_col]):
            df_new[date_col] = pd.to_datetime(df_new[date_col])
        
        df_new[f'{prefix}_year'] = df_new[date_col].dt.year
        df_new[f'{prefix}_month'] = df_new[date_col].dt.month
        df_new[f'{prefix}_day'] = df_new[date_col].dt.day
        df_new[f'{prefix}_day_of_week'] = df_new[date_col].dt.dayofweek  # 0=Mon
        df_new[f'{prefix}_quarter'] = df_new[date_col].dt.quarter
        
        logger.info(f"✅ Extracted date parts for {date_col}")
        return df_new
    
    # ========================================
    # Team 1 - T0016: Date/Time Transformations (intervals)
    # ========================================
    @staticmethod
    def days_between(df: pd.DataFrame,
                    start_col: str,
                    end_col: str,
                    output_col: str = 'days_between') -> pd.DataFrame:
        """
        Calculate days between two date columns
        
        Args:
            df: Input DataFrame
            start_col: Start date column
            end_col: End date column
            output_col: Output column name
        
        Returns:
            DataFrame with new interval column
        """
        df_new = df.copy()
        
        # Ensure datetime
        for col in [start_col, end_col]:
            if not pd.api.types.is_datetime64_any_dtype(df_new[col]):
                df_new[col] = pd.to_datetime(df_new[col])
        
        df_new[output_col] = (df_new[end_col] - df_new[start_col]).dt.days
        
        logger.info(f"✅ Calculated {output_col} between {start_col} and {end_col}")
        return df_new
    
    @staticmethod
    def customer_tenure(sales_df: pd.DataFrame,
                       customer_key: str = 'CustomerKey',
                       order_date_col: str = 'Order Date',
                       reference_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Calculate customer tenure (days since first purchase) and recency
        
        Args:
            sales_df: Sales DataFrame
            customer_key: Customer ID column
            order_date_col: Order date column
            reference_date: Reference date (default: today)
        
        Returns:
            DataFrame with customer tenure and recency
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        sales_copy = sales_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(sales_copy[order_date_col]):
            sales_copy[order_date_col] = pd.to_datetime(sales_copy[order_date_col])
        
        stats = sales_copy.groupby(customer_key)[order_date_col].agg(['min', 'max']).reset_index()
        stats.columns = [customer_key, 'first_purchase_date', 'last_purchase_date']
        
        stats['tenure_days'] = (reference_date - stats['first_purchase_date']).dt.days
        stats['recency_days'] = (reference_date - stats['last_purchase_date']).dt.days
        
        logger.info(f"✅ Calculated tenure and recency for {len(stats)} customers")
        return stats


# Convenience functions
def add_order_delivery_intervals(df: pd.DataFrame,
                                order_col: str = 'Order Date',
                                delivery_col: str = 'Delivery Date') -> pd.DataFrame:
    """
    Add days_between column for order-delivery interval
    """
    return DateTimeEngine.days_between(df, order_col, delivery_col, 'days_order_to_delivery')


def add_customer_tenure(customers_df: pd.DataFrame,
                       sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add tenure and recency to customers DataFrame
    """
    tenure_df = DateTimeEngine.customer_tenure(sales_df)
    
    enhanced = customers_df.merge(tenure_df, on='CustomerKey', how='left')
    
    # Fill NaN for customers with no purchases
    for col in ['tenure_days', 'recency_days']:
        if col in enhanced.columns:
            enhanced[col] = enhanced[col].fillna(0)
    
    return enhanced
