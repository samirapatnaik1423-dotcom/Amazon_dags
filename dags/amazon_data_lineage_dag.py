from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
import json
import os

# ---------------- PATHS ----------------
INPUT_PATH = "/opt/airflow/data/input/amazon_data.csv"
OUTPUT_BASE = "/opt/airflow/data/output/sprint_6"

OUTPUT_DATA = f"{OUTPUT_BASE}/processed_amazon_data.csv"
LINEAGE_META = f"{OUTPUT_BASE}/lineage_metadata.json"
LINEAGE_REPORT = f"{OUTPUT_BASE}/lineage_report.json"

os.makedirs(OUTPUT_BASE, exist_ok=True)

# ---------------- PIPELINE + LINEAGE ----------------
def lineage_pipeline():
    lineage_steps = []

    # Step 1: Dataset Origin
    lineage_steps.append({
        "step": 1,
        "type": "SOURCE",
        "dataset": "amazon_data.csv",
        "path": INPUT_PATH,
        "columns": [
            "OrderID","OrderDate","CustomerID","CustomerName","Email","Age","Phone",
            "ProductID","ProductName","Category","Brand","Quantity","UnitPrice",
            "Discount","Tax","ShippingCost","TotalAmount","PaymentMethod",
            "OrderStatus","City","State","Country","SellerID"
        ]
    })

    # Step 2: Read & Clean
    df = pd.read_csv(INPUT_PATH)
    df.columns = df.columns.str.strip()

    lineage_steps.append({
        "step": 2,
        "type": "READ_CLEAN",
        "description": "Read CSV and normalized column names"
    })

    # Step 3: Transformation
    df["TotalAmount"] = df["TotalAmount"].fillna(0)

    lineage_steps.append({
        "step": 3,
        "type": "TRANSFORMATION",
        "description": "Handled missing TotalAmount values"
    })

    # Step 4: Write Output
    df.to_csv(OUTPUT_DATA, index=False)

    lineage_steps.append({
        "step": 4,
        "type": "WRITE",
        "dataset": "processed_amazon_data.csv",
        "path": OUTPUT_DATA
    })

    # -------- LINEAGE METADATA TABLE --------
    lineage_metadata = {
        "dataset_name": "amazon_orders",
        "pipeline": "amazon_data_lineage_dag",
        "source_path": INPUT_PATH,
        "target_path": OUTPUT_DATA,
        "run_time": datetime.now().isoformat(),
        "lineage_steps": lineage_steps
    }

    with open(LINEAGE_META, "w") as f:
        json.dump(lineage_metadata, f, indent=4)

    # -------- LINEAGE REPORT (VISUAL) --------
    lineage_report = {
        "visual_lineage": [
            "amazon_data.csv",
            "↓ Read & Clean",
            "↓ Transform",
            "↓ Write",
            "processed_amazon_data.csv"
        ],
        "status": "SUCCESS"
    }

    with open(LINEAGE_REPORT, "w") as f:
        json.dump(lineage_report, f, indent=4)

# ---------------- DAG ----------------
with DAG(
    dag_id="amazon_data_lineage_pipeline_sprint_6",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Amazon Dataset Lineage Tracking – Sprint 6"
) as dag:

    lineage_task = PythonOperator(
        task_id="run_lineage_pipeline",
        python_callable=lineage_pipeline
    )

    lineage_task
