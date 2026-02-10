# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 2: Data Cleaning Utilities
# Tasks: T0008-T0011
# ═══════════════════════════════════════════════════════════════════════

"""
cleaning_utils.py - Reusable Data Cleaning Utilities

TASKS IMPLEMENTED:
- T0008: Build reusable cleaning utilities (trim, fillna, typecast)
- T0009: Handle incorrect data types
- T0010: Duplicate data detection & removal
- T0011: Missing data handling strategies (mean, regression, drop)

Methods:
- trim_whitespace(): Remove leading/trailing spaces
- fill_missing_mean(): Fill with column mean
- fill_missing_median(): Fill with column median
- fill_missing_mode(): Fill with mode
- fill_missing_forward(): Forward fill
- fill_missing_backward(): Backward fill
- drop_missing(): Drop rows with missing values
- typecast(): Convert data types
- remove_duplicates(): Remove duplicate rows
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Literal


class DataCleaner:
    """Reusable data cleaning utilities for pandas DataFrames.

    All methods are static and chainable, allowing flexible composition
    of cleaning operations.
    """

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (trim)
    # ========================================
    @staticmethod
    def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
        """Trim leading/trailing whitespace from all string columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with whitespace trimmed from string columns
        """
        df = df.copy()
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            if df[col].dtype == "object":
                df[col] = df[col].str.strip()
        return df

    # ========================================
    # Team 1 - T0011: Missing data handling (mean fill)
    # ========================================
    @staticmethod
    def fill_missing_mean(
        df: pd.DataFrame, columns: list[str] | None = None
    ) -> pd.DataFrame:
        """Fill missing numeric values with column mean.

        Args:
            df: Input DataFrame
            columns: List of columns to fill (if None, fills all numeric columns)

        Returns:
            DataFrame with missing values filled by mean
        """
        df = df.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].mean())
        return df

    # ========================================
    # Team 1 - T0011: Missing data handling (median fill)
    # ========================================
    @staticmethod
    def fill_missing_median(
        df: pd.DataFrame, columns: list[str] | None = None
    ) -> pd.DataFrame:
        """Fill missing numeric values with column median.

        Args:
            df: Input DataFrame
            columns: List of columns to fill (if None, fills all numeric columns)

        Returns:
            DataFrame with missing values filled by median
        """
        df = df.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        return df

    # ========================================
    # Team 1 - T0010: Duplicate data detection & removal
    # ========================================
    @staticmethod
    def remove_duplicates(
        df: pd.DataFrame,
        subset: list[str] | None = None,
        keep: Literal["first", "last", False] = "first",
    ) -> pd.DataFrame:
        """Remove duplicate rows.

        Args:
            df: Input DataFrame
            subset: Column(s) to consider for identifying duplicates
            keep: 'first', 'last', or False (remove all duplicates)

        Returns:
            DataFrame with duplicates removed
        """
        return df.drop_duplicates(subset=subset, keep=keep)

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (typecast)
    # ========================================
    @staticmethod
    def typecast_column(
        df: pd.DataFrame, column: str, dtype: str
    ) -> pd.DataFrame:
        """Cast a column to a specified data type.

        Args:
            df: Input DataFrame
            column: Column name to cast
            dtype: Target data type (e.g., 'int', 'float', 'str', 'datetime')

        Returns:
            DataFrame with column cast to new type
        """
        df = df.copy()
        if column in df.columns:
            if dtype == "int":
                df[column] = df[column].astype("Int64")  # Nullable int
            elif dtype == "float":
                df[column] = df[column].astype("Float64")  # Nullable float
            elif dtype == "str":
                df[column] = df[column].astype(str)
            elif dtype == "datetime":
                df[column] = pd.to_datetime(df[column], errors="coerce")
            else:
                df[column] = df[column].astype(dtype)
        return df

    # ========================================
    # Team 1 - T0009: Handle incorrect data types (email validation)
    # ========================================
    @staticmethod
    def validate_email(
        df: pd.DataFrame,
        column: str,
        regex_pattern: str = r"^[\w\.-]+@[\w\.-]+\.\w+$",
        drop_invalid: bool = False,
    ) -> pd.DataFrame:
        """Validate email format in a column.

        Args:
            df: Input DataFrame
            column: Email column name
            regex_pattern: Email regex pattern
            drop_invalid: If True, drop rows with invalid emails; if False, set to NaN

        Returns:
            DataFrame with email validation applied
        """
        df = df.copy()
        if column not in df.columns:
            return df

        valid_mask = (df[column].isna()) | (
            df[column].str.contains(regex_pattern, regex=True, na=False)
        )

        if drop_invalid:
            df = df[valid_mask]
        else:
            df.loc[~valid_mask, column] = np.nan

        return df

    # ========================================
    # Team 1 - T0008: Build reusable cleaning utilities (empty strings)
    # ========================================
    @staticmethod
    def remove_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Convert empty strings and whitespace-only strings to NaN.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with empty strings replaced by NaN
        """
        df = df.copy()
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            df[col] = df[col].replace(r"^\s*$", np.nan, regex=True)
        return df

    @staticmethod
    def detect_outliers_iqr(
        df: pd.DataFrame, column: str, multiplier: float = 1.5
    ) -> pd.Series:
        """Detect outliers using Interquartile Range (IQR) method.

        Args:
            df: Input DataFrame
            column: Column name to check for outliers
            multiplier: IQR multiplier (default 1.5 for standard definition)

        Returns:
            Boolean Series where True indicates an outlier
        """
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        return (df[column] < lower_bound) | (df[column] > upper_bound)

    @staticmethod
    def remove_outliers_iqr(
        df: pd.DataFrame, columns: list[str] | None = None, multiplier: float = 1.5
    ) -> pd.DataFrame:
        """Remove rows containing outliers (via IQR method).

        Args:
            df: Input DataFrame
            columns: Columns to check for outliers (if None, checks all numeric)
            multiplier: IQR multiplier (default 1.5)

        Returns:
            DataFrame with outlier rows removed
        """
        df = df.copy()
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in columns:
            if col in df.columns:
                outlier_mask = DataCleaner.detect_outliers_iqr(df, col, multiplier)
                df = df[~outlier_mask]

        return df

    @staticmethod
    def handle_missing_data(
        df: pd.DataFrame,
        strategy: Literal["drop", "mean", "median", "forward_fill", "backward_fill"]
        = "drop",
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Handle missing data with various strategies.

        Args:
            df: Input DataFrame
            strategy: 'drop', 'mean', 'median', 'forward_fill', 'backward_fill'
            columns: Columns to apply strategy (if None, applies to all)

        Returns:
            DataFrame with missing data handled
        """
        df = df.copy()

        if columns is None:
            columns = df.columns.tolist()

        if strategy == "drop":
            df = df.dropna(subset=columns)
        elif strategy == "mean":
            for col in columns:
                if col in df.columns and df[col].dtype in [
                    np.float64,
                    np.int64,
                    "float64",
                    "int64",
                ]:
                    df[col] = df[col].fillna(df[col].mean())
        elif strategy == "median":
            for col in columns:
                if col in df.columns and df[col].dtype in [
                    np.float64,
                    np.int64,
                    "float64",
                    "int64",
                ]:
                    df[col] = df[col].fillna(df[col].median())
        elif strategy == "forward_fill":
            df = df.fillna(method="ffill")  # type: ignore
        elif strategy == "backward_fill":
            df = df.fillna(method="bfill")  # type: ignore

        return df
