# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - ETL PIPELINE: TRANSFORM PHASE
# Tasks: T0008-T0017 (Sprint 2 & 3)
# ═══════════════════════════════════════════════════════════════════════

"""
Transform.py - Data Transformation Module

TASKS IMPLEMENTED:
- T0008: Build reusable cleaning utilities (trim, fillna, typecast)
- T0009: Handle incorrect data types
- T0010: Duplicate data detection & removal
- T0011: Missing data handling strategies (mean, regression, drop)
- T0012: Build config-driven cleaning rules
- T0013: Aggregations (groupBy, sum, min, max) - via utils
- T0014: Normalization & scaling - via utils
- T0015: Feature engineering logic - via utils
- T0016: Date/time transformations - via utils
- T0017: Config-based transformation rules - via utils

Responsibilities:
- Clean all 5 tables using table-specific logic
- Apply validation rules
- Standardize formats (dates, types)
- Handle duplicates and missing values
- Apply data transformations

Tables Processed:
1. Customers - Dedupe, birthday→datetime, age→int, email validation
2. Sales - Date standardization, delivery status, Total_Amount_USD
3. Products - Standard cleaning, z-score normalization
4. Stores - Standard cleaning
5. Exchange_Rates - Standard cleaning

Uses utils:
- validation_utils (T0008)
- normalization_utils (T0014)
- datetime_utils (T0016)
- feature_engineering_utils (T0015)
- aggregation_utils (T0013)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import logging
from typing import Dict, Any, Optional, Tuple
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_STAGING = PROJECT_ROOT / "data" / "staging"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"


# ========================================
# TABLE-SPECIFIC CLEANERS (from table_cleaners.py)
# ========================================

class CustomersCleaner:
    """
    TEAM 1 - T0008: Customers table cleaning
    - Remove duplicate rows
    - Standardize birthday format
    - Typecast age to int
    - Fill missing age with mean
    - Validate and fix emails
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean customers table"""
        logger.info("▶ Cleaning Customers table")
        
        stats = {
            'input_rows': len(df),
            'duplicates_removed': 0,
            'birthdays_standardized': 0,
            'ages_typecasted': 0,
            'missing_ages_filled': 0,
            'invalid_emails_fixed': 0
        }
        
        df_clean = df.copy()
        
        # 1. Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['CustomerKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        logger.info(f"   ✅ Removed {stats['duplicates_removed']} duplicate customers")
        
        # 2. Standardize birthday to YYYY-MM-DD
        if 'Birthday' in df_clean.columns:
            df_clean['Birthday'] = pd.to_datetime(df_clean['Birthday'], errors='coerce')
            stats['birthdays_standardized'] = df_clean['Birthday'].notna().sum()
            logger.info(f"   ✅ Standardized {stats['birthdays_standardized']} birthdays")
        
        # 3. Typecast age to int
        if 'Age' in df_clean.columns:
            df_clean['Age'] = pd.to_numeric(df_clean['Age'], errors='coerce')
            missing_ages = df_clean['Age'].isna().sum()
            
            # 4. Fill missing age with mean
            if missing_ages > 0:
                mean_age = df_clean['Age'].mean()
                df_clean['Age'] = df_clean['Age'].fillna(mean_age)
                df_clean['Age'] = df_clean['Age'].astype(int)
                stats['missing_ages_filled'] = missing_ages
                logger.info(f"   ✅ Filled {missing_ages} missing ages with mean: {mean_age:.1f}")
            else:
                df_clean['Age'] = df_clean['Age'].astype(int)
            
            stats['ages_typecasted'] = len(df_clean)
        
        # 5. Validate and fix emails
        if 'Email' in df_clean.columns:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            def validate_email(email):
                if pd.isna(email):
                    return 'no-email@unknown.com'
                if re.match(email_pattern, str(email)):
                    return email
                return 'invalid-email@unknown.com'
            
            invalid_before = (~df_clean['Email'].apply(
                lambda x: bool(re.match(email_pattern, str(x))) if pd.notna(x) else False
            )).sum()
            df_clean['Email'] = df_clean['Email'].apply(validate_email)
            stats['invalid_emails_fixed'] = invalid_before
            logger.info(f"   ✅ Fixed {invalid_before} invalid/missing emails")
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Customers complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class SalesCleaner:
    """
    TEAM 1 - T0009-T0011: Sales table cleaning
    - Standardize dates
    - Add delivery status column
    - JOIN Products to calculate total_amount
    """
    
    @staticmethod
    def clean(sales_df: pd.DataFrame, products_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean sales table with optional Product JOIN"""
        logger.info("▶ Cleaning Sales table")
        
        stats = {
            'input_rows': len(sales_df),
            'dates_standardized': 0,
            'missing_dates_filled': 0,
            'delivery_status_added': 0,
            'total_amount_calculated': 0
        }
        
        df_clean = sales_df.copy()
        
        # 1. Standardize dates
        date_cols = ['Order Date', 'Delivery Date']
        for col in date_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                stats['dates_standardized'] += df_clean[col].notna().sum()
        
        logger.info(f"   ✅ Standardized {stats['dates_standardized']} dates")
        
        # 2. Fill missing delivery dates tracking
        if 'Delivery Date' in df_clean.columns:
            missing_delivery = df_clean['Delivery Date'].isna().sum()
            stats['missing_dates_filled'] = missing_delivery
            logger.info(f"   ℹ️ {missing_delivery} orders with no delivery date")
        
        # 3. Add delivery status column
        df_clean['Delivery_Status'] = df_clean.apply(SalesCleaner._get_delivery_status, axis=1)
        stats['delivery_status_added'] = len(df_clean)
        logger.info(f"   ✅ Added delivery status for {len(df_clean):,} orders")
        
        # 4. JOIN Products to calculate Total_Amount_USD
        if products_df is not None and 'ProductKey' in df_clean.columns:
            if 'Unit Price USD' in products_df.columns:
                # Clean the price column first
                products_clean = products_df[['ProductKey', 'Unit Price USD']].copy()
                products_clean['Unit Price USD'] = pd.to_numeric(
                    products_clean['Unit Price USD'].astype(str).str.replace(r'[^\d.]', '', regex=True),
                    errors='coerce'
                ).fillna(0)
                
                df_clean = df_clean.merge(
                    products_clean,
                    on='ProductKey',
                    how='left'
                )
                
                # Ensure Quantity is numeric
                df_clean['Quantity'] = pd.to_numeric(df_clean['Quantity'], errors='coerce').fillna(0)
                
                df_clean['Total_Amount_USD'] = df_clean['Quantity'] * df_clean['Unit Price USD']
                df_clean['Total_Amount_USD'] = df_clean['Total_Amount_USD'].fillna(0)
                
                stats['total_amount_calculated'] = (df_clean['Total_Amount_USD'] > 0).sum()
                logger.info(f"   ✅ Calculated Total_Amount_USD for {stats['total_amount_calculated']:,} orders")
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Sales complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats
    
    @staticmethod
    def _get_delivery_status(row, reference_date=None):
        """Determine delivery status based on dates"""
        if reference_date is None:
            reference_date = datetime.now()
        
        # If delivered
        if pd.notna(row.get('Delivery Date')):
            return 'Delivered'
        
        # If no order date
        order_date = row.get('Order Date')
        if pd.isna(order_date):
            return 'Unknown'
        
        # Calculate days since order
        if isinstance(order_date, str):
            order_date = pd.to_datetime(order_date)
        
        days_since_order = (reference_date - order_date).days
        
        if days_since_order <= 30:
            return 'Delivering Soon'
        elif days_since_order <= 365:
            return 'To Be Shipped'
        else:
            return 'Lost'


class ProductsCleaner:
    """
    TEAM 1 - T0008: Products table cleaning
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean products table"""
        logger.info("▶ Cleaning Products table")
        
        stats = {'input_rows': len(df), 'duplicates_removed': 0}
        
        df_clean = df.copy()
        
        # Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['ProductKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Products complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class StoresCleaner:
    """
    TEAM 1 - T0008: Stores table cleaning
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean stores table"""
        logger.info("▶ Cleaning Stores table")
        
        stats = {'input_rows': len(df), 'duplicates_removed': 0}
        
        df_clean = df.copy()
        
        # Remove duplicates
        initial_rows = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['StoreKey'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Stores complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


class ExchangeRatesCleaner:
    """
    TEAM 1 - T0008: Exchange Rates table cleaning
    """
    
    @staticmethod
    def clean(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Clean exchange rates table"""
        logger.info("▶ Cleaning Exchange Rates table")
        
        stats = {'input_rows': len(df), 'duplicates_removed': 0, 'dates_standardized': 0}
        
        df_clean = df.copy()
        
        # Standardize date
        if 'Date' in df_clean.columns:
            df_clean['Date'] = pd.to_datetime(df_clean['Date'], errors='coerce')
            stats['dates_standardized'] = df_clean['Date'].notna().sum()
        
        # Remove duplicates
        initial_rows = len(df_clean)
        if 'Date' in df_clean.columns and 'Currency' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=['Date', 'Currency'], keep='first')
        stats['duplicates_removed'] = initial_rows - len(df_clean)
        
        stats['output_rows'] = len(df_clean)
        logger.info(f"✅ Exchange Rates complete: {stats['input_rows']:,} → {stats['output_rows']:,} rows\n")
        
        return df_clean, stats


# ========================================
# MAIN TRANSFORMATION ORCHESTRATOR
# ========================================

class DataTransformer:
    """
    TEAM 1 - Data Transformation Handler
    
    Orchestrates cleaning of all 5 tables.
    """
    
    def __init__(self, staging_dir: Optional[Path] = None, output_dir: Optional[Path] = None):
        """
        Initialize transformer
        
        Args:
            staging_dir: Path to staging directory
            output_dir: Path to processed output directory
        """
        self.staging_dir = Path(staging_dir) if staging_dir else DATA_STAGING
        self.output_dir = Path(output_dir) if output_dir else DATA_PROCESSED
        
        self.cleaned_tables: Dict[str, pd.DataFrame] = {}
        self.cleaning_stats: Dict[str, Dict[str, Any]] = {}
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"▶ DataTransformer initialized")
        logger.info(f"   Staging: {self.staging_dir}")
        logger.info(f"   Output: {self.output_dir}")
    
    def load_from_staging(self, extracted_data: Dict[str, pd.DataFrame] = None) -> Dict[str, pd.DataFrame]:
        """
        Load data from staging or use provided data
        
        Args:
            extracted_data: Optional pre-extracted DataFrames
        
        Returns:
            Dictionary of DataFrames
        """
        if extracted_data:
            return extracted_data
        
        # Load from staging CSVs
        logger.info("▶ Loading from staging...")
        data = {}
        
        staging_files = {
            'customers': 'customers_raw.csv',
            'sales': 'sales_raw.csv',
            'products': 'products_raw.csv',
            'stores': 'stores_raw.csv',
            'exchange_rates': 'exchange_rates_raw.csv'
        }
        
        for table, filename in staging_files.items():
            file_path = self.staging_dir / filename
            if file_path.exists():
                data[table] = pd.read_csv(file_path)
                logger.info(f"   ✅ Loaded {table}: {len(data[table]):,} rows")
            else:
                logger.warning(f"   ⚠️ {filename} not found in staging")
        
        return data
    
    def transform_all(self, extracted_data: Dict[str, pd.DataFrame] = None) -> Dict[str, pd.DataFrame]:
        """
        Clean and transform all tables
        
        Args:
            extracted_data: Optional pre-extracted DataFrames
        
        Returns:
            Dictionary of cleaned DataFrames
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - TRANSFORM PHASE: Cleaning All Tables")
        logger.info("="*70 + "\n")
        
        # Load data
        raw_data = self.load_from_staging(extracted_data)
        
        if not raw_data:
            raise ValueError("No data to transform. Run extraction first.")
        
        # Clean each table in proper order
        # (Products first, needed for Sales JOIN)
        
        # 1. Products
        if 'products' in raw_data:
            self.cleaned_tables['products'], self.cleaning_stats['products'] = \
                ProductsCleaner.clean(raw_data['products'])
        
        # 2. Customers
        if 'customers' in raw_data:
            self.cleaned_tables['customers'], self.cleaning_stats['customers'] = \
                CustomersCleaner.clean(raw_data['customers'])
        
        # 3. Sales (needs Products for JOIN)
        if 'sales' in raw_data:
            products_df = self.cleaned_tables.get('products')
            self.cleaned_tables['sales'], self.cleaning_stats['sales'] = \
                SalesCleaner.clean(raw_data['sales'], products_df)
        
        # 4. Stores
        if 'stores' in raw_data:
            self.cleaned_tables['stores'], self.cleaning_stats['stores'] = \
                StoresCleaner.clean(raw_data['stores'])
        
        # 5. Exchange Rates
        if 'exchange_rates' in raw_data:
            self.cleaned_tables['exchange_rates'], self.cleaning_stats['exchange_rates'] = \
                ExchangeRatesCleaner.clean(raw_data['exchange_rates'])
        
        # Summary
        total_rows = sum(len(df) for df in self.cleaned_tables.values())
        logger.info("-"*50)
        logger.info(f"✅ TRANSFORM COMPLETE: {len(self.cleaned_tables)} tables, {total_rows:,} total rows")
        logger.info("-"*50 + "\n")
        
        return self.cleaned_tables
    
    def save_cleaned_data(self) -> Dict[str, Path]:
        """
        Save cleaned data to processed directory
        
        Returns:
            Dictionary mapping table name to output file path
        """
        logger.info("▶ Saving cleaned data to processed...")
        
        output_files = {}
        
        for table_name, df in self.cleaned_tables.items():
            output_file = self.output_dir / f"{table_name}_cleaned.csv"
            df.to_csv(output_file, index=False)
            output_files[table_name] = output_file
            logger.info(f"   ✅ {table_name} → {output_file.name} ({len(df):,} rows)")
        
        return output_files
    
    def get_transformation_summary(self) -> pd.DataFrame:
        """Get summary of transformation results"""
        if not self.cleaning_stats:
            return pd.DataFrame()
        
        records = []
        for table_name, stats in self.cleaning_stats.items():
            records.append({
                'Table': table_name,
                'Input_Rows': stats.get('input_rows', 0),
                'Output_Rows': stats.get('output_rows', 0),
                'Duplicates_Removed': stats.get('duplicates_removed', 0),
                'Rows_Changed': stats.get('input_rows', 0) - stats.get('output_rows', 0)
            })
        
        return pd.DataFrame(records)


# ========================================
# Airflow-compatible functions
# ========================================

def transform_all_tables(extracted_data: Dict[str, pd.DataFrame] = None, **context) -> Dict[str, Any]:
    """
    Airflow task callable: Transform all tables
    
    Args:
        extracted_data: Optional pre-extracted DataFrames
    
    Returns:
        Dictionary with transformation info
    """
    transformer = DataTransformer()
    
    # Transform all tables
    cleaned = transformer.transform_all(extracted_data)
    
    # Save to processed
    output_files = transformer.save_cleaned_data()
    
    return {
        'tables_cleaned': list(cleaned.keys()),
        'total_rows': sum(len(df) for df in cleaned.values()),
        'output_files': {k: str(v) for k, v in output_files.items()},
        'cleaning_stats': transformer.cleaning_stats,
        'cleaned_tables': cleaned  # Pass along for report generation
    }


def transform_single_table(table_name: str, df: pd.DataFrame, **context) -> Tuple[pd.DataFrame, Dict]:
    """
    Airflow task callable: Transform a single table
    """
    cleaners = {
        'customers': CustomersCleaner,
        'sales': SalesCleaner,
        'products': ProductsCleaner,
        'stores': StoresCleaner,
        'exchange_rates': ExchangeRatesCleaner
    }
    
    if table_name not in cleaners:
        raise ValueError(f"Unknown table: {table_name}")
    
    return cleaners[table_name].clean(df)


# ========================================
# Main execution
# ========================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("TEAM 1 - ETL PIPELINE: TRANSFORM PHASE TEST")
    print("="*70 + "\n")
    
    # First run Extract
    from Extract import DataExtractor
    
    extractor = DataExtractor()
    extracted_data = extractor.extract_all()
    
    # Then Transform
    transformer = DataTransformer()
    cleaned_data = transformer.transform_all(extracted_data)
    
    # Save cleaned data
    output_files = transformer.save_cleaned_data()
    
    # Print summary
    print("\n📊 TRANSFORMATION SUMMARY:")
    print(transformer.get_transformation_summary().to_string(index=False))
    
    print("\n📁 OUTPUT FILES:")
    for table, path in output_files.items():
        print(f"   {table}: {path}")
    
    print("\n✅ Transform phase complete!")
