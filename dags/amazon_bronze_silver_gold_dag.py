from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from typing import Optional
from pydantic import BaseModel, EmailStr, Field

# ---------------- PATHS ----------------
INPUT_PATH = "/opt/airflow/data/input/amazon_data.csv"
OUTPUT_BASE = "/opt/airflow/data/output"

BRONZE_PATH = f"{OUTPUT_BASE}/bronze/amazon_raw.csv"
SILVER_PATH = f"{OUTPUT_BASE}/silver/amazon_cleaned.parquet"
GOLD_PATH = f"{OUTPUT_BASE}/gold/amazon_sales_summary.csv"
ARCHIVE_PATH = f"{OUTPUT_BASE}/archive/amazon_archived.csv"
METADATA_PATH = f"{OUTPUT_BASE}/metadata_log.json"

for layer in ["bronze", "silver", "gold", "archive"]:
    os.makedirs(f"{OUTPUT_BASE}/{layer}", exist_ok=True)

# ---------------- PYDANTIC SCHEMA ----------------
class AmazonOrderSchema(BaseModel):
    OrderID: int
    OrderDate: str
    CustomerID: int
    CustomerName: str
    Email: EmailStr
    Age: Optional[int] = Field(ge=0, le=120)
    Phone: str
    ProductID: int
    ProductName: str
    Category: str
    Brand: str
    Quantity: int
    UnitPrice: float
    Discount: float
    Tax: float
    ShippingCost: float
    TotalAmount: float
    PaymentMethod: str
    OrderStatus: str
    City: str
    State: str
    Country: str
    SellerID: int

EXPECTED_COLUMNS = list(AmazonOrderSchema.__fields__.keys())

# ---------------- PIPELINE LOGIC ----------------
def run_pipeline():
    print("Pipeline started")

    # -------- READ --------
    df = pd.read_csv(INPUT_PATH)
    df.columns = df.columns.str.strip()  # 🔥 FIX 1

    # -------- BRONZE --------
    df.to_csv(BRONZE_PATH, index=False)
    print("Bronze layer completed")

    # -------- SILVER --------
    drift_log = {"missing_columns": [], "extra_columns": []}

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None
            drift_log["missing_columns"].append(col)

    for col in df.columns:
        if col not in EXPECTED_COLUMNS:
            drift_log["extra_columns"].append(col)

    df = df[EXPECTED_COLUMNS]

    valid_rows = []
    failed = 0

    for _, row in df.iterrows():
        try:
            AmazonOrderSchema(**row.to_dict())
            valid_rows.append(row)
        except Exception:
            failed += 1

    silver_df = pd.DataFrame(valid_rows)

    if not silver_df.empty:
        silver_df.to_parquet(SILVER_PATH, index=False)
    else:
        pd.DataFrame(columns=EXPECTED_COLUMNS).to_parquet(SILVER_PATH, index=False)

    print("Silver layer completed")

    # -------- GOLD (SAFE) --------
    if "Category" in silver_df.columns and not silver_df.empty:
        gold_df = (
            silver_df
            .groupby("Category", as_index=False)
            .agg(
                total_sales=("TotalAmount", "sum"),
                total_quantity=("Quantity", "sum")
            )
        )
    else:
        gold_df = pd.DataFrame(
            columns=["Category", "total_sales", "total_quantity"]
        )

    gold_df.to_csv(GOLD_PATH, index=False)
    print("Gold layer completed")

    # -------- ARCHIVE --------
    if "OrderDate" in silver_df.columns:
        archived_df = silver_df[silver_df["OrderDate"] < "2025-01-01"]
    else:
        archived_df = pd.DataFrame(columns=EXPECTED_COLUMNS)

    archived_df.to_csv(ARCHIVE_PATH, index=False)

    # -------- METADATA --------
    metadata = {
        "dataset": "amazon_orders",
        "records_read": len(df),
        "records_silver": len(silver_df),
        "records_gold": len(gold_df),
        "records_archived": len(archived_df),
        "records_failed": failed,
        "formats": {
            "bronze": "csv",
            "silver": "parquet",
            "gold": "csv"
        },
        "schema_drift": drift_log,
        "run_time": datetime.now().isoformat(),
        "status": "SUCCESS"
    }

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=4)

    print("Pipeline finished successfully")

# ---------------- DAG ----------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="amazon_bronze_silver_gold_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),  # 🔥 FIX 2
    description="Amazon Bronze-Silver-Gold Data Pipeline"
) as dag:

    run_pipeline_task = PythonOperator(
        task_id="run_bronze_silver_gold_pipeline",
        python_callable=run_pipeline
    )

    run_pipeline_task
