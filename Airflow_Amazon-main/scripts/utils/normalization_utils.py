# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 3: Normalization Utilities
# Tasks: T0014
# ═══════════════════════════════════════════════════════════════════════

"""
Normalization Utilities - Data Normalization and Scaling
Part of T0014: Normalization & Scaling

Provides:
- Z-score standardization (mean=0, std=1) as NEW columns (preserves originals)
- Auto-detection of numeric columns
- Batch processing for memory optimization
- Column-level scaling control

NOTE: Z-score is added as a NEW column (e.g., Quantity -> Quantity_zscore)
      Original values are PRESERVED for analysis
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class NormalizationEngine:
    """
    Data normalization and scaling operations
    Focus on Z-score standardization as per requirements
    """
    
    # ========================================
    # Team 1 - T0014: Normalization & Scaling (Z-score)
    # ========================================
    @staticmethod
    def z_score_normalize(df: pd.DataFrame,
                         columns: Optional[List[str]] = None,
                         exclude_columns: Optional[List[str]] = None,
                         batch_size: Optional[int] = None,
                         add_as_new_column: bool = True,
                         suffix: str = '_zscore') -> Tuple[pd.DataFrame, Dict[str, Dict[str, float]]]:
        """
        Apply Z-score standardization (mean=0, std=1)
        
        IMPORTANT: By default, z-score is added as a NEW column (e.g., Quantity -> Quantity_zscore)
                   Original values are PRESERVED for analysis
        
        Formula: z = (x - mean) / std
        
        Args:
            df: Input DataFrame
            columns: Specific columns to normalize (default: all numeric)
            exclude_columns: Columns to exclude from normalization
            batch_size: Optional batch size for memory optimization
            add_as_new_column: If True (default), adds z-score as new column, preserving original
            suffix: Suffix for new z-score columns (default: '_zscore')
        
        Returns:
            (normalized_df, normalization_stats)
            normalization_stats contains mean and std for each column
        
        Example:
            >>> normalized_df, stats = NormalizationEngine.z_score_normalize(df)
            >>> # Original 'Quantity' preserved, 'Quantity_zscore' added
            >>> # To reverse: original = normalized * std + mean
        """
        df_normalized = df.copy()
        stats = {}
        
        # Auto-detect numeric columns if not specified
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
            # Exclude key columns that shouldn't be normalized
            key_columns = ['CustomerKey', 'ProductKey', 'StoreKey', 'Order Number']
            columns = [col for col in columns if col not in key_columns]
            logger.info(f"Auto-detected {len(columns)} numeric columns for normalization")
        
        # Remove excluded columns
        if exclude_columns:
            columns = [col for col in columns if col not in exclude_columns]
            logger.info(f"Excluded {len(exclude_columns)} columns from normalization")
        
        if batch_size:
            logger.info(f"Processing in batches of {batch_size} rows")
            df_normalized, stats = NormalizationEngine._z_score_batched(
                df_normalized, columns, batch_size, add_as_new_column, suffix
            )
        else:
            # Standard normalization - ADD as new column (preserve original)
            for col in columns:
                if col not in df_normalized.columns:
                    logger.warning(f"⚠️ Column not found: {col}")
                    continue
                
                mean_val = df_normalized[col].mean()
                std_val = df_normalized[col].std()
                
                if std_val == 0:
                    logger.warning(f"⚠️ Column {col} has std=0, skipping normalization")
                    stats[col] = {'mean': mean_val, 'std': std_val, 'skipped': True}
                    continue
                
                # Determine target column name
                if add_as_new_column:
                    target_col = f"{col}{suffix}"
                else:
                    target_col = col
                
                # Apply Z-score normalization to NEW column (preserving original)
                df_normalized[target_col] = (df_normalized[col] - mean_val) / std_val
                
                stats[col] = {
                    'original_column': col,
                    'zscore_column': target_col,
                    'mean': float(mean_val),
                    'std': float(std_val),
                    'min': float(df_normalized[target_col].min()),
                    'max': float(df_normalized[target_col].max()),
                    'skipped': False
                }
                
                logger.info(f"✅ Added {target_col}: z-score of {col} (mean={mean_val:.2f}, std={std_val:.2f})")
        
        logger.info(f"✅ Z-score normalization complete: {len(stats)} new columns added")
        return df_normalized, stats
    
    @staticmethod
    def _z_score_batched(df: pd.DataFrame,
                        columns: List[str],
                        batch_size: int,
                        add_as_new_column: bool = True,
                        suffix: str = '_zscore') -> Tuple[pd.DataFrame, Dict[str, Dict[str, float]]]:
        """
        Apply Z-score normalization in batches for memory efficiency
        
        Note: Calculates overall mean/std first, then applies in batches
              Adds z-score as NEW column by default (preserves original)
        
        Args:
            df: Input DataFrame
            columns: Columns to normalize
            batch_size: Batch size
            add_as_new_column: If True, adds z-score as new column
            suffix: Suffix for z-score columns
        
        Returns:
            (normalized_df, stats)
        """
        stats = {}
        
        # Calculate overall statistics first (needed for consistent normalization)
        for col in columns:
            target_col = f"{col}{suffix}" if add_as_new_column else col
            stats[col] = {
                'original_column': col,
                'zscore_column': target_col,
                'mean': float(df[col].mean()),
                'std': float(df[col].std())
            }
            # Initialize the new z-score column
            if add_as_new_column:
                df[target_col] = 0.0
        
        # Apply normalization in batches
        total_batches = (len(df) + batch_size - 1) // batch_size
        
        for i in range(0, len(df), batch_size):
            batch_end = min(i + batch_size, len(df))
            logger.info(f"  Processing batch {i//batch_size + 1}/{total_batches}")
            
            for col in columns:
                mean_val = stats[col]['mean']
                std_val = stats[col]['std']
                target_col = stats[col]['zscore_column']
                
                if std_val != 0:
                    df.loc[df.index[i:batch_end], target_col] = (
                        (df.loc[df.index[i:batch_end], col] - mean_val) / std_val
                    )
        
        # Add min/max to stats
        for col in columns:
            target_col = stats[col]['zscore_column']
            stats[col]['min'] = float(df[target_col].min())
            stats[col]['max'] = float(df[target_col].max())
            stats[col]['skipped'] = stats[col]['std'] == 0
        
        return df, stats
    
    @staticmethod
    def denormalize(df: pd.DataFrame,
                   stats: Dict[str, Dict[str, float]],
                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Reverse Z-score normalization to get original values
        
        Formula: x = z * std + mean
        
        Args:
            df: Normalized DataFrame
            stats: Normalization statistics from z_score_normalize()
            columns: Columns to denormalize (default: all in stats)
        
        Returns:
            Denormalized DataFrame
        """
        df_denormalized = df.copy()
        
        if columns is None:
            columns = list(stats.keys())
        
        for col in columns:
            if col not in stats:
                logger.warning(f"⚠️ No stats found for {col}, skipping")
                continue
            
            if stats[col].get('skipped', False):
                logger.info(f"Column {col} was skipped during normalization")
                continue
            
            mean_val = stats[col]['mean']
            std_val = stats[col]['std']
            
            # Reverse normalization
            df_denormalized[col] = df_denormalized[col] * std_val + mean_val
            
            logger.info(f"✅ Denormalized {col}")
        
        return df_denormalized
    
    @staticmethod
    def get_normalization_report(stats: Dict[str, Dict[str, float]]) -> pd.DataFrame:
        """
        Generate normalization report from statistics
        
        Args:
            stats: Normalization statistics
        
        Returns:
            Report DataFrame
        """
        report_data = []
        
        for col, col_stats in stats.items():
            report_data.append({
                'column': col,
                'original_mean': col_stats['mean'],
                'original_std': col_stats['std'],
                'normalized_min': col_stats.get('min', None),
                'normalized_max': col_stats.get('max', None),
                'skipped': col_stats.get('skipped', False)
            })
        
        report = pd.DataFrame(report_data)
        logger.info(f"Generated normalization report for {len(report)} columns")
        
        return report
    
    @staticmethod
    def save_normalization_stats(stats: Dict[str, Dict[str, float]],
                                output_path: str) -> bool:
        """
        Save normalization statistics to JSON file for later use
        
        Args:
            stats: Normalization statistics
            output_path: Output JSON file path
        
        Returns:
            Success boolean
        """
        import json
        from pathlib import Path
        
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(stats, f, indent=2)
            
            logger.info(f"✅ Saved normalization stats: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save stats: {e}")
            return False
    
    @staticmethod
    def load_normalization_stats(input_path: str) -> Dict[str, Dict[str, float]]:
        """
        Load normalization statistics from JSON file
        
        Args:
            input_path: Input JSON file path
        
        Returns:
            Normalization statistics
        """
        import json
        
        try:
            with open(input_path, 'r') as f:
                stats = json.load(f)
            
            logger.info(f"✅ Loaded normalization stats: {input_path}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to load stats: {e}")
            return {}
    
    @staticmethod
    def check_normalization_quality(df: pd.DataFrame,
                                   columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Check if DataFrame is properly normalized (mean≈0, std≈1)
        
        Args:
            df: Normalized DataFrame to check
            columns: Columns to check (default: all numeric)
        
        Returns:
            Quality report DataFrame
        """
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        quality_data = []
        
        for col in columns:
            mean_val = df[col].mean()
            std_val = df[col].std()
            
            # Check if close to normalized (mean≈0, std≈1)
            is_normalized = (abs(mean_val) < 0.1 and abs(std_val - 1.0) < 0.1)
            
            quality_data.append({
                'column': col,
                'mean': float(mean_val),
                'std': float(std_val),
                'is_normalized': is_normalized,
                'status': '✅' if is_normalized else '❌'
            })
        
        quality_report = pd.DataFrame(quality_data)
        
        normalized_count = quality_report['is_normalized'].sum()
        logger.info(f"Normalization quality: {normalized_count}/{len(quality_report)} columns properly normalized")
        
        return quality_report


# Convenience functions
def quick_normalize(df: pd.DataFrame,
                   exclude_keys: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    Quick normalization with smart defaults
    
    Args:
        df: Input DataFrame
        exclude_keys: Exclude columns with 'Key' or 'ID' in name
    
    Returns:
        (normalized_df, stats)
    """
    exclude_columns = []
    
    if exclude_keys:
        # Exclude key columns
        exclude_columns = [col for col in df.columns 
                          if 'key' in col.lower() or 'id' in col.lower()]
        logger.info(f"Excluding {len(exclude_columns)} key/ID columns")
    
    return NormalizationEngine.z_score_normalize(df, exclude_columns=exclude_columns)


def normalize_features(df: pd.DataFrame,
                      feature_columns: List[str]) -> Tuple[pd.DataFrame, Dict]:
    """
    Normalize specific feature columns
    
    Args:
        df: Input DataFrame
        feature_columns: List of feature column names
    
    Returns:
        (normalized_df, stats)
    """
    return NormalizationEngine.z_score_normalize(df, columns=feature_columns)
