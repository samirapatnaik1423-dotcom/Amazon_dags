from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
import os
import json

# ---------------- PATHS ----------------
RAW_PATH = "/opt/airflow/data/input/amazon_data.csv"
CLEAN_PATH = "/opt/airflow/data/output/amazon_cleaned.csv"
DQ_REPORT_PATH = "/opt/airflow/data/output/amazon_dq_report.json"

# ---------------- EXTRACT ----------------
def extract():
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError("Amazon data file not found")

    df = pd.read_csv(
        RAW_PATH,
        sep=None,
        engine="python",
        dtype=str
    )

    print("Extracted columns:", df.columns.tolist())
    df.to_csv(CLEAN_PATH, index=False)

# ---------------- TRANSFORM ----------------
def transform():
    df = pd.read_csv(CLEAN_PATH, dtype=str)

    # Clean column names
    df.columns = df.columns.str.strip()

    # Replace empty strings with NaN
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Numeric columns (UPDATED)
    numeric_cols = [
        "Age",
        "Quantity",
        "UnitPrice",
        "Discount",
        "Tax",
        "ShippingCost",
        "TotalAmount"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date conversion
    if "OrderDate" in df.columns:
        df["OrderDate"] = pd.to_datetime(df["OrderDate"], errors="coerce")

    # Email cleaning (UPDATED)
    if "Email" in df.columns:
        df["Email"] = df["Email"].str.lower().str.strip()

    df.to_csv(CLEAN_PATH, index=False)

# ---------------- DATA QUALITY CHECKS ----------------
def data_quality_checks():
    df = pd.read_csv(CLEAN_PATH)

    dq_report = {}

    # -------- NULL CHECKS --------
    dq_report["null_checks"] = {
        col: int(df[col].isnull().sum())
        for col in df.columns
    }

    # -------- UNIQUENESS CHECKS --------
    dq_report["uniqueness_checks"] = {
        "OrderID_unique": df["OrderID"].is_unique if "OrderID" in df.columns else None,
        "Email_unique": df["Email"].is_unique if "Email" in df.columns else None
    }

    # -------- DATA PROFILING --------
    profiling = {}

    for col in df.columns:
        profiling[col] = {
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].count()),
            "unique_values": int(df[col].nunique())
        }

        if pd.api.types.is_numeric_dtype(df[col]):
            profiling[col].update({
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "mean": float(df[col].mean())
            })

    dq_report["data_profiling"] = profiling

    # -------- SUMMARY --------
    dq_report["summary"] = {
        "total_rows": len(df),
        "total_columns": len(df.columns)
    }

    # Save report
    with open(DQ_REPORT_PATH, "w") as f:
        json.dump(dq_report, f, indent=4)

    print("Data Quality Report Generated Successfully")

# ---------------- DAG ----------------
with DAG(
    dag_id="amazon_etl_data_quality_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["amazon", "etl", "data-quality"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_amazon_data",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_amazon_data",
        python_callable=transform
    )

    dq_task = PythonOperator(
        task_id="amazon_data_quality_checks",
        python_callable=data_quality_checks
    )

    extract_task >> transform_task >> dq_task
