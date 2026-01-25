from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
import json
import os
import time
import re

INPUT_FILE = "/opt/airflow/data/input/amazon_data.csv"
OUTPUT_PATH = "/opt/airflow/data/output/dq_api"

os.makedirs(OUTPUT_PATH, exist_ok=True)

def start_timer(**context):
    context["ti"].xcom_push(key="start_time", value=time.time())

def run_data_quality_checks(**context):
    df = pd.read_csv(INPUT_FILE)

    # 🔥 STANDARDIZE COLUMN NAMES (safety)
    df.columns = df.columns.str.strip()

    # 🔥 CLEAN & CAST NUMERIC COLUMNS (CRITICAL FIX)
    numeric_cols = [
        "Age", "Quantity", "UnitPrice",
        "Discount", "Tax", "ShippingCost", "TotalAmount"
    ]

    for col in numeric_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace('"', '', regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    total_rows = len(df)
    valid_rows = 0
    rejected_records = []

    for _, row in df.iterrows():

        email_valid = bool(
            re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", str(row["Email"]))
        )

        age_valid = pd.notna(row["Age"]) and row["Age"] >= 18
        quantity_valid = pd.notna(row["Quantity"]) and row["Quantity"] > 0
        price_valid = pd.notna(row["UnitPrice"]) and row["UnitPrice"] > 0
        total_valid = pd.notna(row["TotalAmount"]) and row["TotalAmount"] > 0

        status_valid = row["OrderStatus"] in [
            "Delivered", "Cancelled", "Pending", "Returned", "Shipped"
        ]

        if all([
            email_valid,
            age_valid,
            quantity_valid,
            price_valid,
            total_valid,
            status_valid
        ]):
            valid_rows += 1
        else:
            rejected_records.append(row)

    dq_score = round((valid_rows / total_rows) * 100, 2)

    # Save rejected rows
    pd.DataFrame(rejected_records).to_csv(
        f"{OUTPUT_PATH}/rejected_rows.csv", index=False
    )

    # Save DQ score
    with open(f"{OUTPUT_PATH}/dq_score.json", "w") as f:
        json.dump({"dq_score": dq_score}, f)

    # Save validation summary
    summary = {
        "total_records": total_rows,
        "valid_records": valid_rows,
        "rejected_records": len(rejected_records)
    }

    with open(f"{OUTPUT_PATH}/validation_summary.json", "w") as f:
        json.dump(summary, f)

def pipeline_metrics(**context):
    start_time = context["ti"].xcom_pull(key="start_time")
    end_time = time.time()

    metrics = {
        "pipeline_status": "SUCCESS",
        "execution_time_seconds": round(end_time - start_time, 2)
    }

    with open(f"{OUTPUT_PATH}/pipeline_metrics.json", "w") as f:
        json.dump(metrics, f)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="dq_api_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start = PythonOperator(
        task_id="start_timer",
        python_callable=start_timer
    )

    dq_task = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks
    )

    metrics_task = PythonOperator(
        task_id="pipeline_metrics",
        python_callable=pipeline_metrics
    )

    start >> dq_task >> metrics_task
