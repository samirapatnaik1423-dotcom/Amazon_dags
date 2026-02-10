# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Feature Engineering Utilities
# Tasks: T0015
# ═══════════════════════════════════════════════════════════════════════

"""
Feature Engineering Utilities - Create New Features from Existing Data
Part of T0015: Feature Engineering Logic

Provides:
- Customer features: lifetime_value, purchase_frequency, avg_order_value, recency, tenure
- Sales features: revenue metrics, product rankings, performance scores
- Product features: popularity score, sales velocity, revenue contribution
- Automatic feature addition to DataFrame or separate feature table
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class FeatureEngine:
    """
    Feature engineering for customers, sales, and products
    """
    
    ############# CUSTOMER FEATURES #############
    
    # ========================================
    # Team 1 - T0015: Feature Engineering (customer features)
    # ========================================
    @staticmethod
    def create_customer_features(customers_df: pd.DataFrame,
                                 sales_df: pd.DataFrame,
                                 customer_key: str = 'CustomerKey',
                                 order_date_col: str = 'Order Date',
                                 quantity_col: str = 'Quantity',
                                 reference_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Create comprehensive customer features
        
        Features created:
        - total_orders: Total number of orders
        - total_quantity: Total quantity purchased
        - avg_order_quantity: Average quantity per order
        - lifetime_value: Total purchase value (if available)
        - purchase_frequency: Orders per month
        - recency_days: Days since last purchase
        - tenure_days: Days since first purchase
        - is_active: Active in last 90 days
        
        Args:
            customers_df: Customer master DataFrame
            sales_df: Sales transaction DataFrame
            customer_key: Customer ID column name
            order_date_col: Order date column name
            quantity_col: Quantity column name
            reference_date: Reference date for recency (default: today)
        
        Returns:
            Customer DataFrame with new features
        """
        logger.info(f"Creating customer features for {len(customers_df)} customers")
        
        customers_enhanced = customers_df.copy()
        
        if reference_date is None:
            reference_date = datetime.now()
        
        # Ensure date column is datetime
        sales_copy = sales_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(sales_copy[order_date_col]):
            sales_copy[order_date_col] = pd.to_datetime(sales_copy[order_date_col])
        
        # Aggregate sales by customer
        customer_stats = sales_copy.groupby(customer_key).agg({
            order_date_col: ['count', 'min', 'max'],
            quantity_col: ['sum', 'mean']
        }).reset_index()
        
        # Flatten column names
        customer_stats.columns = [
            customer_key,
            'total_orders',
            'first_purchase_date',
            'last_purchase_date',
            'total_quantity',
            'avg_order_quantity'
        ]
        
        # Calculate derived features
        customer_stats['tenure_days'] = (
            reference_date - customer_stats['first_purchase_date']
        ).dt.days
        
        customer_stats['recency_days'] = (
            reference_date - customer_stats['last_purchase_date']
        ).dt.days
        
        customer_stats['purchase_frequency'] = (
            customer_stats['total_orders'] / 
            (customer_stats['tenure_days'] / 30.0 + 1)  # +1 to avoid division by zero
        )
        
        customer_stats['is_active'] = (customer_stats['recency_days'] <= 90).astype(int)
        
        # Merge with customers
        customers_enhanced = customers_enhanced.merge(
            customer_stats[[customer_key, 'total_orders', 'total_quantity', 
                          'avg_order_quantity', 'tenure_days', 'recency_days', 
                          'purchase_frequency', 'is_active']],
            on=customer_key,
            how='left'
        )
        
        # Fill NaN for customers with no purchases
        feature_cols = ['total_orders', 'total_quantity', 'avg_order_quantity', 
                       'tenure_days', 'recency_days', 'purchase_frequency', 'is_active']
        for col in feature_cols:
            if col in customers_enhanced.columns:
                customers_enhanced[col] = customers_enhanced[col].fillna(0)
        
        logger.info(f"✅ Created {len(feature_cols)} customer features")
        return customers_enhanced
    
    @staticmethod
    def calculate_customer_lifetime_value(sales_df: pd.DataFrame,
                                          customer_key: str = 'CustomerKey',
                                          revenue_col: Optional[str] = None,
                                          quantity_col: str = 'Quantity',
                                          price_col: Optional[str] = None) -> pd.DataFrame:
        """
        Calculate customer lifetime value (CLV)
        
        Args:
            sales_df: Sales DataFrame
            customer_key: Customer ID column
            revenue_col: Revenue column (if available)
            quantity_col: Quantity column
            price_col: Unit price column (if available)
        
        Returns:
            DataFrame with CustomerKey and lifetime_value
        """
        sales_copy = sales_df.copy()
        
        # Calculate revenue if not provided
        if revenue_col and revenue_col in sales_copy.columns:
            clv = sales_copy.groupby(customer_key)[revenue_col].sum().reset_index()
            clv.columns = [customer_key, 'lifetime_value']
        elif price_col and price_col in sales_copy.columns:
            sales_copy['revenue'] = sales_copy[quantity_col] * sales_copy[price_col]
            clv = sales_copy.groupby(customer_key)['revenue'].sum().reset_index()
            clv.columns = [customer_key, 'lifetime_value']
        else:
            # Use quantity as proxy
            clv = sales_copy.groupby(customer_key)[quantity_col].sum().reset_index()
            clv.columns = [customer_key, 'lifetime_value']
            logger.warning("⚠️ No revenue column found, using quantity as CLV proxy")
        
        logger.info(f"✅ Calculated CLV for {len(clv)} customers")
        return clv
    
    ############# SALES FEATURES #############
    
    @staticmethod
    def create_sales_features(sales_df: pd.DataFrame,
                             products_df: Optional[pd.DataFrame] = None,
                             product_key: str = 'ProductKey',
                             quantity_col: str = 'Quantity') -> pd.DataFrame:
        """
        Create sales-level features
        
        Features created:
        - order_size_category: Small/Medium/Large based on quantity
        - is_bulk_order: Flag for large orders
        - product_price (if products_df provided)
        - order_revenue (if products_df provided)
        
        Args:
            sales_df: Sales DataFrame
            products_df: Product master DataFrame (optional)
            product_key: Product ID column
            quantity_col: Quantity column
        
        Returns:
            Sales DataFrame with new features
        """
        logger.info(f"Creating sales features for {len(sales_df)} transactions")
        
        sales_enhanced = sales_df.copy()
        
        # Order size categorization
        quantity_75 = sales_enhanced[quantity_col].quantile(0.75)
        quantity_25 = sales_enhanced[quantity_col].quantile(0.25)
        
        def categorize_order_size(qty):
            if qty >= quantity_75:
                return 'Large'
            elif qty <= quantity_25:
                return 'Small'
            else:
                return 'Medium'
        
        sales_enhanced['order_size_category'] = sales_enhanced[quantity_col].apply(categorize_order_size)
        sales_enhanced['is_bulk_order'] = (sales_enhanced[quantity_col] >= quantity_75).astype(int)
        
        # Add product information if available
        if products_df is not None and product_key in products_df.columns:
            logger.info("Merging product information")
            
            # Get price columns if available
            price_cols = [col for col in products_df.columns if 'price' in col.lower() or 'cost' in col.lower()]
            
            if price_cols:
                merge_cols = [product_key] + price_cols
                sales_enhanced = sales_enhanced.merge(
                    products_df[merge_cols],
                    on=product_key,
                    how='left'
                )
                
                # Calculate revenue if unit price available
                if 'Unit Price USD' in sales_enhanced.columns:
                    sales_enhanced['order_revenue'] = (
                        sales_enhanced[quantity_col] * sales_enhanced['Unit Price USD']
                    )
                    logger.info("✅ Calculated order revenue")
        
        logger.info(f"✅ Created sales features")
        return sales_enhanced
    
    ############# PRODUCT FEATURES #############
    
    @staticmethod
    def create_product_features(products_df: pd.DataFrame,
                               sales_df: pd.DataFrame,
                               product_key: str = 'ProductKey',
                               quantity_col: str = 'Quantity') -> pd.DataFrame:
        """
        Create product-level features
        
        Features created:
        - total_units_sold: Total quantity sold
        - total_orders: Number of orders
        - avg_order_quantity: Average quantity per order
        - popularity_score: Relative popularity (0-100)
        - sales_rank: Rank by total units sold
        - is_top_seller: Top 20% products
        
        Args:
            products_df: Product master DataFrame
            sales_df: Sales transaction DataFrame
            product_key: Product ID column
            quantity_col: Quantity column
        
        Returns:
            Product DataFrame with new features
        """
        logger.info(f"Creating product features for {len(products_df)} products")
        
        products_enhanced = products_df.copy()
        
        # Aggregate sales by product
        product_stats = sales_df.groupby(product_key).agg({
            quantity_col: ['sum', 'count', 'mean']
        }).reset_index()
        
        product_stats.columns = [product_key, 'total_units_sold', 'total_orders', 'avg_order_quantity']
        
        # Calculate popularity score (0-100)
        max_units = product_stats['total_units_sold'].max()
        product_stats['popularity_score'] = (
            (product_stats['total_units_sold'] / max_units * 100)
            .round(2)
        )
        
        # Sales rank
        product_stats['sales_rank'] = product_stats['total_units_sold'].rank(
            ascending=False,
            method='min'
        ).astype(int)
        
        # Top seller flag (top 20%)
        top_20_threshold = product_stats['total_units_sold'].quantile(0.80)
        product_stats['is_top_seller'] = (
            product_stats['total_units_sold'] >= top_20_threshold
        ).astype(int)
        
        # Merge with products
        products_enhanced = products_enhanced.merge(
            product_stats,
            on=product_key,
            how='left'
        )
        
        # Fill NaN for products with no sales
        feature_cols = ['total_units_sold', 'total_orders', 'avg_order_quantity', 
                       'popularity_score', 'sales_rank', 'is_top_seller']
        for col in feature_cols:
            if col in products_enhanced.columns:
                products_enhanced[col] = products_enhanced[col].fillna(0)
        
        logger.info(f"✅ Created {len(feature_cols)} product features")
        return products_enhanced
    
    ############# GENERAL FEATURE UTILITIES #############
    
    @staticmethod
    def save_features(df: pd.DataFrame,
                     output_path: Optional[str] = None,
                     db_manager: Optional[Any] = None,
                     table_name: Optional[str] = None,
                     save_to_csv: bool = True,
                     save_to_db: bool = False) -> Dict[str, bool]:
        """
        Save engineered features to CSV and/or database
        
        Args:
            df: DataFrame with features
            output_path: CSV output path
            db_manager: DatabaseManager instance
            table_name: Database table name
            save_to_csv: Save to CSV file
            save_to_db: Save to database
        
        Returns:
            Dict with save status {'csv': bool, 'db': bool}
        """
        from pathlib import Path
        
        result = {'csv': False, 'db': False}
        
        # Save to CSV
        if save_to_csv and output_path:
            try:
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(output_path, index=False, encoding='utf-8')
                logger.info(f"✅ Saved features to CSV: {output_path}")
                result['csv'] = True
            except Exception as e:
                logger.error(f"❌ Failed to save CSV: {e}")
        
        # Save to database
        if save_to_db and db_manager and table_name:
            try:
                db_manager.insert_dataframe(df, table_name)
                logger.info(f"✅ Saved features to database table: {table_name}")
                result['db'] = True
            except Exception as e:
                logger.error(f"❌ Failed to save to database: {e}")
        
        return result
    
    @staticmethod
    def get_feature_report(original_df: pd.DataFrame,
                          enhanced_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate report of newly created features
        
        Args:
            original_df: Original DataFrame
            enhanced_df: Enhanced DataFrame with new features
        
        Returns:
            Report DataFrame
        """
        original_cols = set(original_df.columns)
        enhanced_cols = set(enhanced_df.columns)
        
        new_features = enhanced_cols - original_cols
        
        report_data = []
        for feature in new_features:
            report_data.append({
                'feature_name': feature,
                'data_type': str(enhanced_df[feature].dtype),
                'null_count': int(enhanced_df[feature].isnull().sum()),
                'null_percent': round(enhanced_df[feature].isnull().sum() / len(enhanced_df) * 100, 2),
                'unique_values': int(enhanced_df[feature].nunique()),
                'sample_values': str(enhanced_df[feature].head(3).tolist())
            })
        
        report = pd.DataFrame(report_data)
        logger.info(f"Feature report: {len(report)} new features created")
        
        return report


# Convenience functions
def quick_customer_features(customers_df: pd.DataFrame,
                           sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Quick customer feature creation with defaults
    
    Args:
        customers_df: Customer DataFrame
        sales_df: Sales DataFrame
    
    Returns:
        Customer DataFrame with features
    """
    return FeatureEngine.create_customer_features(customers_df, sales_df)


def quick_product_features(products_df: pd.DataFrame,
                          sales_df: pd.DataFrame) -> pd.DataFrame:
    """
    Quick product feature creation with defaults
    
    Args:
        products_df: Product DataFrame
        sales_df: Sales DataFrame
    
    Returns:
        Product DataFrame with features
    """
    return FeatureEngine.create_product_features(products_df, sales_df)
