# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - ETL PIPELINE: REPORT GENERATION
# Tasks: T0031 (Sprint 6)
# ═══════════════════════════════════════════════════════════════════════

"""
ReportGenerator.py - Report Generation Module

TASKS IMPLEMENTED:
- T0031: Pipeline execution summary - Generate comprehensive reports

Responsibilities:
- Generate 9 comprehensive reports from cleaned data
- Output reports to CSV and database
- Track report generation metrics

Reports Generated:
1. Customer Summary - Demographics, age distribution, location
2. Product Performance - Best/worst sellers, category analysis
3. Order Status - Delivered/Delivering Soon/To Be Shipped/Lost counts
4. Sales Trends (Daily) - Revenue by day/month, growth rates
5. Data Quality Scorecard - Completeness %, validity %, issues
6. Customer Segmentation - RFM analysis
7. Store Performance - Revenue by store, regional analysis
8. Anomaly Detection - Outliers, suspicious patterns
9. DAG Execution Summary - Pipeline run metrics

Uses utils:
- aggregation_utils (T0013)
- dag_execution_tracker (T0031)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"


# ========================================
# INDIVIDUAL REPORT GENERATORS
# ========================================

class CustomerSummaryReport:
    """Report 1: Customer Summary with demographics and metrics"""
    
    @staticmethod
    def generate(customers_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate customer summary report"""
        logger.info("▶ Generating Customer Summary Report")
        
        reference_date = datetime.now()
        
        # Ensure datetime
        sales_df = sales_df.copy()
        sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
        if 'Delivery Date' in sales_df.columns:
            sales_df['Delivery Date'] = pd.to_datetime(sales_df['Delivery Date'])
        
        # Aggregate by customer
        customer_agg = sales_df.groupby('CustomerKey').agg({
            'Order Number': 'count',
            'Quantity': 'sum',
            'Total_Amount_USD': 'sum',
            'Order Date': ['min', 'max']
        }).reset_index()
        
        customer_agg.columns = [
            'CustomerKey', 'Total_Orders', 'Total_Quantity',
            'Total_Spend_USD', 'First_Order_Date', 'Last_Order_Date'
        ]
        
        # Calculate metrics
        customer_agg['Avg_Order_Value'] = customer_agg['Total_Spend_USD'] / customer_agg['Total_Orders']
        customer_agg['Customer_Tenure_Days'] = (reference_date - customer_agg['First_Order_Date']).dt.days
        customer_agg['Recency_Days'] = (reference_date - customer_agg['Last_Order_Date']).dt.days
        customer_agg['Order_Frequency'] = customer_agg['Total_Orders'] / (customer_agg['Customer_Tenure_Days'] / 30.0 + 1)
        customer_agg['Is_Active'] = (customer_agg['Recency_Days'] <= 90).astype(int)
        
        # Customer segmentation
        def segment_customer(row):
            if row['Recency_Days'] > 365:
                return 'Inactive'
            elif row['Total_Orders'] == 1:
                return 'New'
            elif row['Total_Spend_USD'] > 5000 and row['Total_Orders'] > 10:
                return 'Gold'
            elif row['Total_Spend_USD'] > 2000 and row['Total_Orders'] > 5:
                return 'Silver'
            elif row['Recency_Days'] > 180 and row['Total_Orders'] > 5:
                return 'At_Risk'
            else:
                return 'Bronze'
        
        customer_agg['Customer_Segment'] = customer_agg.apply(segment_customer, axis=1)
        
        logger.info(f"✅ Customer summary: {len(customer_agg):,} customers")
        return customer_agg


class ProductPerformanceReport:
    """Report 2: Product Performance analysis"""
    
    @staticmethod
    def generate(products_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate product performance report"""
        logger.info("▶ Generating Product Performance Report")
        
        # Aggregate by product
        product_agg = sales_df.groupby('ProductKey').agg({
            'Quantity': 'sum',
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'Order Date': 'max'
        }).reset_index()
        
        product_agg.columns = [
            'ProductKey', 'Total_Units_Sold', 'Total_Orders',
            'Total_Revenue_USD', 'Last_Sold_Date'
        ]
        
        # Merge with product details
        product_report = products_df.merge(product_agg, on='ProductKey', how='left')
        product_report['Total_Units_Sold'] = product_report['Total_Units_Sold'].fillna(0)
        product_report['Total_Orders'] = product_report['Total_Orders'].fillna(0)
        product_report['Total_Revenue_USD'] = product_report['Total_Revenue_USD'].fillna(0)
        
        # Calculate metrics
        product_report['Avg_Order_Quantity'] = np.where(
            product_report['Total_Orders'] > 0,
            product_report['Total_Units_Sold'] / product_report['Total_Orders'],
            0
        )
        
        product_report['Sales_Rank'] = product_report['Total_Revenue_USD'].rank(
            ascending=False, method='min'
        ).astype(int)
        product_report['Is_Top_Seller'] = (
            product_report['Sales_Rank'] <= len(product_report) * 0.1
        ).astype(int)
        
        logger.info(f"✅ Product performance: {len(product_report):,} products")
        return product_report


class OrderStatusReport:
    """Report 3: Order Status distribution"""
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate order status summary"""
        logger.info("▶ Generating Order Status Report")
        
        status_agg = sales_df.groupby('Delivery_Status').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        status_agg.columns = [
            'Delivery_Status', 'Order_Count', 'Total_Value_USD', 'Customers_Affected'
        ]
        
        status_agg['Pct_Of_Total_Orders'] = (
            status_agg['Order_Count'] / status_agg['Order_Count'].sum() * 100
        ).round(2)
        
        logger.info(f"✅ Order status: {len(status_agg)} statuses")
        return status_agg


class SalesTrendsReport:
    """Report 4: Daily Sales Trends"""
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate daily sales trends"""
        logger.info("▶ Generating Sales Trends (Daily) Report")
        
        sales_df = sales_df.copy()
        sales_df['Order Date'] = pd.to_datetime(sales_df['Order Date'])
        sales_df['Date'] = sales_df['Order Date'].dt.date
        
        # Aggregate by date
        daily_sales = sales_df.groupby('Date').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'Quantity': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        daily_sales.columns = [
            'Date', 'Total_Orders', 'Total_Revenue_USD',
            'Total_Quantity', 'Unique_Customers'
        ]
        
        daily_sales['Avg_Order_Value'] = daily_sales['Total_Revenue_USD'] / daily_sales['Total_Orders']
        
        # Date features
        daily_sales['Date'] = pd.to_datetime(daily_sales['Date'])
        daily_sales['Day_Of_Week'] = daily_sales['Date'].dt.dayofweek
        daily_sales['Is_Weekend'] = (daily_sales['Day_Of_Week'] >= 5).astype(int)
        daily_sales['Month'] = daily_sales['Date'].dt.month_name()
        daily_sales['Quarter'] = daily_sales['Date'].dt.quarter
        
        logger.info(f"✅ Sales trends: {len(daily_sales)} days")
        return daily_sales


class DataQualityScorecard:
    """Report 5: Data Quality Scorecard"""
    
    @staticmethod
    def generate(cleaning_stats: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
        """Generate data quality scorecard from cleaning stats"""
        logger.info("▶ Generating Data Quality Scorecard")
        
        records = []
        for table_name, stats in cleaning_stats.items():
            input_rows = stats.get('input_rows', 0)
            output_rows = stats.get('output_rows', 0)
            
            records.append({
                'Table': table_name.title(),
                'Input_Rows': input_rows,
                'Output_Rows': output_rows,
                'Duplicates_Removed': stats.get('duplicates_removed', 0),
                'Rows_Cleaned': input_rows - output_rows,
                'Dates_Standardized': stats.get('dates_standardized', 0),
                'Emails_Fixed': stats.get('invalid_emails_fixed', 0),
                'Missing_Values_Filled': stats.get('missing_ages_filled', 0),
                'Quality_Score': round(output_rows / input_rows * 100, 2) if input_rows > 0 else 0,
                'Validation_Status': 'PASS' if output_rows > 0 else 'FAIL'
            })
        
        scorecard = pd.DataFrame(records)
        logger.info(f"✅ Data quality scorecard: {len(scorecard)} tables")
        return scorecard


class CustomerSegmentationReport:
    """Report 6: Customer Segmentation (RFM)"""
    
    @staticmethod
    def generate(customer_summary_df: pd.DataFrame) -> pd.DataFrame:
        """Generate RFM segmentation summary"""
        logger.info("▶ Generating Customer Segmentation (RFM) Report")
        
        df = customer_summary_df.copy()
        
        # Calculate RFM scores (1-5) with error handling
        try:
            df['R_Score'] = pd.qcut(df['Recency_Days'], q=5, labels=[5, 4, 3, 2, 1], duplicates='drop')
        except ValueError:
            df['R_Score'] = 3  # Default if not enough unique values
        
        try:
            df['F_Score'] = pd.qcut(df['Total_Orders'], q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
        except ValueError:
            df['F_Score'] = 3
        
        try:
            df['M_Score'] = pd.qcut(df['Total_Spend_USD'], q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
        except ValueError:
            df['M_Score'] = 3
        
        # Convert to int
        df['R_Score'] = df['R_Score'].astype(int)
        df['F_Score'] = df['F_Score'].astype(int)
        df['M_Score'] = df['M_Score'].astype(int)
        
        df['RFM_Score'] = df['R_Score'] * 100 + df['F_Score'] * 10 + df['M_Score']
        
        # Segment distribution
        segment_summary = df.groupby('Customer_Segment').agg({
            'CustomerKey': 'count',
            'Total_Spend_USD': 'sum',
            'Total_Orders': 'sum',
            'Recency_Days': 'mean'
        }).reset_index()
        
        segment_summary.columns = [
            'Segment', 'Customer_Count', 'Total_Revenue_USD',
            'Total_Orders', 'Avg_Recency_Days'
        ]
        
        logger.info(f"✅ Customer segmentation: {len(segment_summary)} segments")
        return segment_summary


class StorePerformanceReport:
    """Report 7: Store Performance"""
    
    @staticmethod
    def generate(stores_df: pd.DataFrame, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Generate store performance report"""
        logger.info("▶ Generating Store Performance Report")
        
        # Aggregate by store
        store_agg = sales_df.groupby('StoreKey').agg({
            'Order Number': 'count',
            'Total_Amount_USD': 'sum',
            'CustomerKey': 'nunique'
        }).reset_index()
        
        store_agg.columns = [
            'StoreKey', 'Total_Orders', 'Total_Revenue_USD', 'Total_Customers'
        ]
        
        store_agg['Avg_Order_Value'] = store_agg['Total_Revenue_USD'] / store_agg['Total_Orders']
        
        # Merge with store details
        store_report = stores_df.merge(store_agg, on='StoreKey', how='left')
        store_report[['Total_Orders', 'Total_Revenue_USD', 'Total_Customers']] = \
            store_report[['Total_Orders', 'Total_Revenue_USD', 'Total_Customers']].fillna(0)
        
        # Delivery success rate
        if 'Delivery_Status' in sales_df.columns:
            delivered = sales_df[sales_df['Delivery_Status'] == 'Delivered'].groupby('StoreKey').size()
            delivered = delivered.reset_index(name='Delivered_Count')
            store_report = store_report.merge(delivered, on='StoreKey', how='left')
            store_report['Delivered_Count'] = store_report['Delivered_Count'].fillna(0)
            store_report['Delivery_Success_Rate'] = np.where(
                store_report['Total_Orders'] > 0,
                (store_report['Delivered_Count'] / store_report['Total_Orders'] * 100).round(2),
                0
            )
        
        logger.info(f"✅ Store performance: {len(store_report):,} stores")
        return store_report


class AnomalyDetectionReport:
    """Report 8: Anomaly Detection"""
    
    @staticmethod
    def generate(sales_df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies in sales data"""
        logger.info("▶ Generating Anomaly Detection Report")
        
        anomalies = []
        
        # 1. Large order anomalies (Quantity > 3 std dev)
        qty_mean = sales_df['Quantity'].mean()
        qty_std = sales_df['Quantity'].std()
        large_orders = sales_df[sales_df['Quantity'] > qty_mean + 3 * qty_std]
        
        for _, row in large_orders.head(50).iterrows():
            anomalies.append({
                'Order_Number': row['Order Number'],
                'Anomaly_Type': 'Large_Order',
                'Description': f"Quantity {row['Quantity']} exceeds 3 std dev",
                'Severity': 'High',
                'Value': row['Quantity']
            })
        
        # 2. Price spike anomalies
        if 'Total_Amount_USD' in sales_df.columns:
            amount_mean = sales_df['Total_Amount_USD'].mean()
            amount_std = sales_df['Total_Amount_USD'].std()
            price_spikes = sales_df[sales_df['Total_Amount_USD'] > amount_mean + 3 * amount_std]
            
            for _, row in price_spikes.head(50).iterrows():
                anomalies.append({
                    'Order_Number': row['Order Number'],
                    'Anomaly_Type': 'Price_Spike',
                    'Description': f"Amount ${row['Total_Amount_USD']:.2f} exceeds 3 std dev",
                    'Severity': 'High',
                    'Value': row['Total_Amount_USD']
                })
        
        anomaly_report = pd.DataFrame(anomalies)
        logger.info(f"✅ Anomaly detection: {len(anomaly_report)} anomalies")
        return anomaly_report


class DAGExecutionReport:
    """Report 9: DAG Execution Summary"""
    
    @staticmethod
    def generate(extraction_stats: Dict = None,
                 cleaning_stats: Dict = None,
                 load_stats: Dict = None) -> pd.DataFrame:
        """Generate DAG execution summary"""
        logger.info("▶ Generating DAG Execution Summary")
        
        records = []
        timestamp = datetime.now()
        
        # Extraction phase
        if extraction_stats:
            for table, stats in extraction_stats.items():
                records.append({
                    'Phase': 'Extract',
                    'Table': table,
                    'Rows_Processed': stats.get('rows', 0),
                    'Duration_Sec': stats.get('duration_seconds', 0),
                    'Status': 'SUCCESS' if stats.get('success') else 'FAILED',
                    'Timestamp': timestamp
                })
        
        # Cleaning phase
        if cleaning_stats:
            for table, stats in cleaning_stats.items():
                records.append({
                    'Phase': 'Transform',
                    'Table': table,
                    'Rows_Processed': stats.get('output_rows', 0),
                    'Duration_Sec': 0,  # Not tracked in transform
                    'Status': 'SUCCESS' if stats.get('output_rows', 0) > 0 else 'FAILED',
                    'Timestamp': timestamp
                })
        
        # Load phase
        if load_stats:
            for table, stats in load_stats.items():
                records.append({
                    'Phase': 'Load',
                    'Table': table,
                    'Rows_Processed': stats.get('loaded_rows', 0),
                    'Duration_Sec': stats.get('duration_seconds', 0),
                    'Status': 'SUCCESS' if stats.get('success') else 'FAILED',
                    'Timestamp': timestamp
                })
        
        summary = pd.DataFrame(records)
        logger.info(f"✅ DAG execution summary: {len(summary)} entries")
        return summary


# ========================================
# MAIN REPORT ORCHESTRATOR
# ========================================

class ReportOrchestrator:
    """
    TEAM 1 - Report Generation Handler
    
    Generates all 9 reports from cleaned data.
    """
    
    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize report generator
        
        Args:
            output_dir: Path to reports output directory
        """
        self.output_dir = Path(output_dir) if output_dir else REPORTS_DIR
        self.reports: Dict[str, pd.DataFrame] = {}
        self.generation_stats: Dict[str, Dict[str, Any]] = {}
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"▶ ReportOrchestrator initialized")
        logger.info(f"   Output: {self.output_dir}")
    
    def generate_all_reports(self,
                            cleaned_tables: Dict[str, pd.DataFrame],
                            cleaning_stats: Dict[str, Dict[str, Any]] = None,
                            extraction_stats: Dict = None,
                            load_stats: Dict = None) -> Dict[str, pd.DataFrame]:
        """
        Generate all 9 reports
        
        Args:
            cleaned_tables: Dictionary of cleaned DataFrames
            cleaning_stats: Statistics from transform phase
            extraction_stats: Statistics from extract phase
            load_stats: Statistics from load phase
        
        Returns:
            Dictionary of report DataFrames
        """
        logger.info("\n" + "="*70)
        logger.info("TEAM 1 - REPORT GENERATION: Creating All Reports")
        logger.info("="*70 + "\n")
        
        customers_df = cleaned_tables.get('customers')
        sales_df = cleaned_tables.get('sales')
        products_df = cleaned_tables.get('products')
        stores_df = cleaned_tables.get('stores')
        
        # Generate each report
        try:
            # 1. Customer Summary
            if customers_df is not None and sales_df is not None:
                self.reports['customer_summary'] = CustomerSummaryReport.generate(
                    customers_df, sales_df
                )
            
            # 2. Product Performance
            if products_df is not None and sales_df is not None:
                self.reports['product_performance'] = ProductPerformanceReport.generate(
                    products_df, sales_df
                )
            
            # 3. Order Status
            if sales_df is not None:
                self.reports['order_status'] = OrderStatusReport.generate(sales_df)
            
            # 4. Sales Trends
            if sales_df is not None:
                self.reports['sales_trends_daily'] = SalesTrendsReport.generate(sales_df)
            
            # 5. Data Quality Scorecard
            if cleaning_stats:
                self.reports['data_quality_scorecard'] = DataQualityScorecard.generate(
                    cleaning_stats
                )
            
            # 6. Customer Segmentation (RFM)
            if 'customer_summary' in self.reports:
                self.reports['customer_segmentation'] = CustomerSegmentationReport.generate(
                    self.reports['customer_summary']
                )
            
            # 7. Store Performance
            if stores_df is not None and sales_df is not None:
                self.reports['store_performance'] = StorePerformanceReport.generate(
                    stores_df, sales_df
                )
            
            # 8. Anomaly Detection
            if sales_df is not None:
                self.reports['anomaly_detection'] = AnomalyDetectionReport.generate(sales_df)
            
            # 9. DAG Execution Summary
            self.reports['dag_execution_summary'] = DAGExecutionReport.generate(
                extraction_stats, cleaning_stats, load_stats
            )
            
        except Exception as e:
            logger.error(f"❌ Report generation failed: {e}")
            raise
        
        logger.info("-"*50)
        logger.info(f"✅ REPORTS COMPLETE: {len(self.reports)} reports generated")
        logger.info("-"*50 + "\n")
        
        return self.reports
    
    def save_reports(self) -> Dict[str, Path]:
        """
        Save all reports to CSV
        
        Returns:
            Dictionary mapping report name to file path
        """
        logger.info("▶ Saving reports to CSV...")
        
        output_files = {}
        
        for report_name, df in self.reports.items():
            if df is not None and len(df) > 0:
                output_file = self.output_dir / f"{report_name}.csv"
                df.to_csv(output_file, index=False)
                output_files[report_name] = output_file
                logger.info(f"   ✅ {report_name} → {output_file.name} ({len(df)} rows)")
        
        return output_files
    
    def get_report_summary(self) -> pd.DataFrame:
        """Get summary of generated reports"""
        records = []
        for report_name, df in self.reports.items():
            records.append({
                'Report': report_name,
                'Rows': len(df) if df is not None else 0,
                'Columns': len(df.columns) if df is not None else 0
            })
        return pd.DataFrame(records)


# ========================================
# Airflow-compatible functions
# ========================================

def generate_all_reports(cleaned_tables: Dict[str, pd.DataFrame],
                        cleaning_stats: Dict = None,
                        extraction_stats: Dict = None,
                        load_stats: Dict = None,
                        **context) -> Dict[str, Any]:
    """
    Airflow task callable: Generate all reports
    
    Returns:
        Dictionary with report info
    """
    orchestrator = ReportOrchestrator()
    
    # Generate all reports
    reports = orchestrator.generate_all_reports(
        cleaned_tables, cleaning_stats, extraction_stats, load_stats
    )
    
    # Save to CSV
    output_files = orchestrator.save_reports()
    
    return {
        'reports_generated': list(reports.keys()),
        'output_files': {k: str(v) for k, v in output_files.items()},
        'summary': orchestrator.get_report_summary().to_dict()
    }


# ========================================
# Main execution
# ========================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("TEAM 1 - ETL PIPELINE: REPORT GENERATION TEST")
    print("="*70 + "\n")
    
    # Run full pipeline
    from Extract import DataExtractor
    from Transform import DataTransformer
    
    # Extract
    extractor = DataExtractor()
    extracted_data = extractor.extract_all()
    
    # Transform
    transformer = DataTransformer()
    cleaned_data = transformer.transform_all(extracted_data)
    transformer.save_cleaned_data()
    
    # Generate Reports
    orchestrator = ReportOrchestrator()
    reports = orchestrator.generate_all_reports(
        cleaned_data,
        cleaning_stats=transformer.cleaning_stats,
        extraction_stats=extractor.extraction_stats
    )
    
    # Save reports
    output_files = orchestrator.save_reports()
    
    # Print summary
    print("\n📊 REPORT SUMMARY:")
    print(orchestrator.get_report_summary().to_string(index=False))
    
    print("\n📁 REPORT FILES:")
    for report, path in output_files.items():
        print(f"   {report}: {path}")
    
    print("\n✅ Report generation complete!")
