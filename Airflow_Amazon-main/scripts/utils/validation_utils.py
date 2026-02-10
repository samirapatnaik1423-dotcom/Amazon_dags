# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 2: Data Validation Utilities
# Tasks: T0008, T0012
# ═══════════════════════════════════════════════════════════════════════

"""
Validation Utilities - Data Type Detection & Quality Validation
Part of T0008: Build Reusable Cleaning Utilities

Provides:
- Data type detection (int, float, string, date, bool)
- Null/missing value analysis
- Email format validation
- Duplicate key detection
- Value range validation
- Quality check execution
"""

import pandas as pd
import numpy as np
from typing import Tuple, List, Dict, Any, Optional
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)


class DataValidator:
    """Reusable data validation utilities"""
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (detection)
    # ========================================
    @staticmethod
    def detect_data_types(df: pd.DataFrame) -> Dict[str, str]:
        """
        Detect and infer data types for columns
        
        Returns:
            Dictionary of {column: inferred_type}
        """
        type_map = {}
        
        for col in df.columns:
            # Skip if all nulls
            if df[col].isnull().all():
                type_map[col] = 'unknown'
                continue
            
            # Get non-null values
            non_null = df[col].dropna()
            
            # Check for numeric
            try:
                pd.to_numeric(non_null, errors='coerce')
                numeric_valid = pd.to_numeric(non_null, errors='coerce').notna().sum()
                if numeric_valid / len(non_null) > 0.9:  # >90% convertible to numeric
                    # Check if integer or float
                    try:
                        int(non_null.iloc[0])
                        type_map[col] = 'int'
                    except (ValueError, TypeError):
                        type_map[col] = 'float'
                    continue
            except:
                pass
            
            # Check for date
            if col.lower() in ['date', 'birthday', 'order_date', 'delivery_date', 'created_at', 'updated_at']:
                type_map[col] = 'date'
                continue
            
            # Check if bool
            unique_vals = non_null.unique()
            if len(unique_vals) <= 2 and all(v in [True, False, 0, 1, 'yes', 'no', 'Y', 'N'] for v in unique_vals):
                type_map[col] = 'bool'
                continue
            
            # Default to string
            type_map[col] = 'string'
        
        return type_map
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (email validation)
    # ========================================
    @staticmethod
    def validate_email(email: str) -> bool:
        """Validate email format"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, str(email)))
    
    # ========================================
    # PHASE 1 - T0008: Column-Level Validation (Regex, Min/Max)
    # ========================================
    @staticmethod
    def validate_column_rules(df: pd.DataFrame, column: str, rules: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate column against multiple rules
        
        Args:
            df: DataFrame to validate
            column: Column name
            rules: Dict with keys: regex, min_value, max_value, allowed_values, not_null
        
        Returns:
            (is_valid, validation_details)
        """
        results = {
            'column': column,
            'total_rows': len(df),
            'passed': 0,
            'failed': 0,
            'failures': []
        }
        
        if column not in df.columns:
            return False, {'error': f'Column {column} not found'}
        
        col_data = df[column].dropna()  # Skip nulls unless not_null rule exists
        
        # Check not_null rule
        if rules.get('not_null', False):
            null_count = df[column].isnull().sum()
            if null_count > 0:
                results['failures'].append(f'{null_count} null values found')
                results['failed'] += null_count
        
        # Check regex pattern
        if 'regex' in rules and len(col_data) > 0:
            pattern = re.compile(rules['regex'])
            invalid = col_data[~col_data.astype(str).str.match(pattern)]
            if len(invalid) > 0:
                results['failures'].append(f'{len(invalid)} values failed regex')
                results['failed'] += len(invalid)
        
        # Check min_value
        if 'min_value' in rules:
            try:
                numeric_col = pd.to_numeric(col_data, errors='coerce')
                below_min = numeric_col[numeric_col < rules['min_value']].count()
                if below_min > 0:
                    results['failures'].append(f'{below_min} values below minimum {rules["min_value"]}')
                    results['failed'] += below_min
            except:
                pass
        
        # Check max_value
        if 'max_value' in rules:
            try:
                numeric_col = pd.to_numeric(col_data, errors='coerce')
                above_max = numeric_col[numeric_col > rules['max_value']].count()
                if above_max > 0:
                    results['failures'].append(f'{above_max} values above maximum {rules["max_value"]}')
                    results['failed'] += above_max
            except:
                pass
        
        # Check allowed_values
        if 'allowed_values' in rules:
            invalid_vals = col_data[~col_data.isin(rules['allowed_values'])]
            if len(invalid_vals) > 0:
                results['failures'].append(f'{len(invalid_vals)} values not in allowed list')
                results['failed'] += len(invalid_vals)
        
        results['passed'] = results['total_rows'] - results['failed']
        is_valid = results['failed'] == 0
        
        return is_valid, results
    
    # ========================================
    # PHASE 1 - T0009: Data Profiling Framework
    # ========================================
    @staticmethod
    def profile_dataframe(df: pd.DataFrame, table_name: str = 'unknown') -> Dict[str, Any]:
        """
        Generate comprehensive statistical profile of DataFrame
        
        Returns:
            Dict with profiling statistics
        """
        profile = {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'columns': {},
            'generated_at': datetime.now().isoformat()
        }
        
        for col in df.columns:
            col_profile = {
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percent': round((df[col].isnull().sum() / len(df)) * 100, 2),
                'unique_count': int(df[col].nunique()),
                'cardinality': round((df[col].nunique() / len(df)) * 100, 2)
            }
            
            # Numeric statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    'mean': float(df[col].mean()) if not df[col].isnull().all() else None,
                    'median': float(df[col].median()) if not df[col].isnull().all() else None,
                    'std': float(df[col].std()) if not df[col].isnull().all() else None,
                    'min': float(df[col].min()) if not df[col].isnull().all() else None,
                    'max': float(df[col].max()) if not df[col].isnull().all() else None,
                    'q25': float(df[col].quantile(0.25)) if not df[col].isnull().all() else None,
                    'q75': float(df[col].quantile(0.75)) if not df[col].isnull().all() else None
                })
            
            # Top values (for categorical)
            if col_profile['cardinality'] < 50:  # Low cardinality
                top_vals = df[col].value_counts().head(5).to_dict()
                col_profile['top_values'] = {str(k): int(v) for k, v in top_vals.items()}
            
            profile['columns'][col] = col_profile
        
        return profile
    
    # ========================================
    # PHASE 1 - T0011: Anomaly Detection
    # ========================================
    @staticmethod
    def detect_anomalies_zscore(df: pd.DataFrame, column: str, threshold: float = 3.0) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Detect outliers using Z-score method
        
        Args:
            df: DataFrame
            column: Numeric column to check
            threshold: Z-score threshold (default 3.0 = 99.7% confidence)
        
        Returns:
            (anomalies_df, summary_dict)
        """
        if column not in df.columns:
            return pd.DataFrame(), {'error': f'Column {column} not found'}
        
        # Calculate Z-scores
        col_data = pd.to_numeric(df[column], errors='coerce').dropna()
        mean = col_data.mean()
        std = col_data.std()
        
        if std == 0:
            return pd.DataFrame(), {'error': 'Standard deviation is zero'}
        
        z_scores = np.abs((col_data - mean) / std)
        
        # Find anomalies
        anomaly_mask = z_scores > threshold
        anomalies = df.loc[col_data[anomaly_mask].index].copy()
        anomalies['z_score'] = z_scores[anomaly_mask].values
        anomalies['deviation_from_mean'] = (col_data[anomaly_mask] - mean).values
        
        summary = {
            'column': column,
            'total_rows': len(df),
            'anomaly_count': len(anomalies),
            'anomaly_percent': round((len(anomalies) / len(df)) * 100, 2),
            'threshold': threshold,
            'mean': float(mean),
            'std': float(std),
            'min_anomaly_value': float(anomalies[column].min()) if len(anomalies) > 0 else None,
            'max_anomaly_value': float(anomalies[column].max()) if len(anomalies) > 0 else None
        }
        
        return anomalies, summary
    
    # ========================================
    # Team 1 - T0012: Config-driven cleaning rules (null validation)
    # ========================================
    @staticmethod
    def validate_column_nulls(df: pd.DataFrame, column: str, allow_null: bool = False) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate null values in a column
        
        Returns:
            (is_valid, details)
        """
        null_count = df[column].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        
        is_valid = allow_null or (null_count == 0)
        
        return is_valid, {
            'column': column,
            'null_count': int(null_count),
            'null_percent': round(null_pct, 2),
            'total_rows': len(df)
        }
    
    # ========================================
    # Team 1 - T0009: Handle incorrect data types (type matching)
    # ========================================
    @staticmethod
    def validate_type_match(series: pd.Series, expected_type: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate if series matches expected type
        
        Args:
            series: Pandas series
            expected_type: 'int', 'float', 'string', 'date', 'bool'
        
        Returns:
            (is_valid, details)
        """
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return True, {'matches': 0, 'total': 0, 'match_percent': 100}
        
        if expected_type == 'int':
            try:
                matches = pd.to_numeric(non_null, errors='coerce').notna().sum()
                # Check if actually integers (no decimals)
                int_matches = sum(1 for x in non_null if isinstance(x, (int, np.integer)) or (isinstance(x, (str, float)) and str(x).replace('.0', '').isdigit()))
                match_pct = (int_matches / len(non_null)) * 100
            except:
                match_pct = 0
        
        elif expected_type == 'float':
            try:
                matches = pd.to_numeric(non_null, errors='coerce').notna().sum()
                match_pct = (matches / len(non_null)) * 100
            except:
                match_pct = 0
        
        elif expected_type == 'date':
            try:
                pd.to_datetime(non_null, errors='coerce')
                matches = pd.to_datetime(non_null, errors='coerce').notna().sum()
                match_pct = (matches / len(non_null)) * 100
            except:
                match_pct = 0
        
        elif expected_type == 'string':
            match_pct = 100  # Everything can be string
        
        else:
            match_pct = 0
        
        is_valid = match_pct >= 90  # >90% match
        
        return is_valid, {
            'column': series.name,
            'expected_type': expected_type,
            'match_percent': round(match_pct, 2),
            'matches': int(matches) if expected_type != 'string' else len(non_null),
            'total_non_null': len(non_null)
        }
    
    @staticmethod
    def validate_value_range(series: pd.Series, min_value: Optional[float] = None, 
                            max_value: Optional[float] = None) -> Tuple[bool, Dict[str, Any]]:
        """Validate numeric values are within range"""
        numeric_series = pd.to_numeric(series, errors='coerce')
        valid_count = 0
        invalid_count = 0
        
        for val in numeric_series.dropna():
            if min_value is not None and val < min_value:
                invalid_count += 1
                continue
            if max_value is not None and val > max_value:
                invalid_count += 1
                continue
            valid_count += 1
        
        is_valid = invalid_count == 0
        
        return is_valid, {
            'column': series.name,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'min_value': min_value,
            'max_value': max_value
        }
    
    @staticmethod
    def validate_email_column(series: pd.Series, allow_null: bool = False) -> Tuple[bool, Dict[str, Any]]:
        """Validate email addresses in a column"""
        valid_count = 0
        invalid_count = 0
        null_count = series.isnull().sum()
        
        for email in series.dropna():
            if DataValidator.validate_email(email):
                valid_count += 1
            else:
                invalid_count += 1
        
        is_valid = invalid_count == 0
        
        return is_valid, {
            'column': series.name,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'null_count': int(null_count),
            'allow_null': allow_null
        }
    
    @staticmethod
    def validate_duplicate_keys(df: pd.DataFrame, key_columns: List[str]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate for duplicates on specific columns
        
        Returns:
            (is_unique, details)
        """
        if not key_columns:
            return True, {'duplicates': 0, 'total_rows': len(df)}
        
        # Check if all key columns exist
        missing_cols = [col for col in key_columns if col not in df.columns]
        if missing_cols:
            return False, {'error': f'Missing columns: {missing_cols}'}
        
        duplicates = df.duplicated(subset=key_columns, keep=False).sum()
        duplicate_rows = df[df.duplicated(subset=key_columns, keep=False)].index.tolist()
        
        is_unique = duplicates == 0
        
        return is_unique, {
            'key_columns': key_columns,
            'duplicate_rows_count': int(duplicates),
            'duplicate_indices': duplicate_rows[:100],  # First 100
            'total_rows': len(df)
        }
    
    @staticmethod
    def run_quality_checks(df: pd.DataFrame, checks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run a series of quality checks on the dataframe
        
        Args:
            df: Input DataFrame
            checks: List of check configurations
        
        Returns:
            Results of all checks
        """
        results = {
            'total_checks': len(checks),
            'passed': 0,
            'failed': 0,
            'details': []
        }
        
        for check in checks:
            check_type = check.get('check')
            column = check.get('column')
            
            try:
                if check_type == 'no_nulls':
                    is_valid, details = DataValidator.validate_column_nulls(df, column, allow_null=False)
                    results['details'].append({
                        'check': check_type,
                        'column': column,
                        'passed': is_valid,
                        'details': details
                    })
                
                elif check_type == 'type_match':
                    expected_type = check.get('expected_type')
                    is_valid, details = DataValidator.validate_type_match(df[column], expected_type)
                    results['details'].append({
                        'check': check_type,
                        'column': column,
                        'passed': is_valid,
                        'details': details
                    })
                
                elif check_type == 'email_validity':
                    is_valid, details = DataValidator.validate_email_column(df[column])
                    results['details'].append({
                        'check': check_type,
                        'column': column,
                        'passed': is_valid,
                        'details': details
                    })
                
                elif check_type == 'positive_values':
                    min_val = check.get('min_value', 0)
                    is_valid, details = DataValidator.validate_value_range(df[column], min_value=min_val)
                    results['details'].append({
                        'check': check_type,
                        'column': column,
                        'passed': is_valid,
                        'details': details
                    })
                
                elif check_type == 'date_format':
                    # Basic date format check
                    fmt = check.get('format', '%Y-%m-%d')
                    is_valid = True
                    try:
                        pd.to_datetime(df[column], format=fmt, errors='coerce')
                    except:
                        is_valid = False
                    results['details'].append({
                        'check': check_type,
                        'column': column,
                        'passed': is_valid,
                        'format': fmt
                    })
                
                if results['details'][-1]['passed']:
                    results['passed'] += 1
                else:
                    results['failed'] += 1
            
            except Exception as e:
                results['details'].append({
                    'check': check_type,
                    'column': column,
                    'error': str(e)
                })
                results['failed'] += 1
        
        return results
