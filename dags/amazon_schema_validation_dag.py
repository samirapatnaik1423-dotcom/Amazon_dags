from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
import json
import os
from typing import Optional
from pydantic import BaseModel, EmailStr, Field, validator

# ---------------- PATHS ----------------
INPUT_PATH = "/opt/airflow/data/input/amazon_data.csv"
OUTPUT_DIR = "/opt/airflow/data/output"
OUTPUT_DATA = f"{OUTPUT_DIR}/amazon_validated.csv"
DRIFT_LOG = f"{OUTPUT_DIR}/schema_drift_log.json"
SCHEMA_VERSIONS = f"{OUTPUT_DIR}/schema_versions.json"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------- PYDANTIC SCHEMA ----------------
class AmazonOrderSchema(BaseModel):
    OrderID: int
    OrderDate: datetime
    CustomerID: int
    CustomerName: str
    Email: EmailStr
    Age: Optional[int] = Field(ge=0, le=120)
    Phone: str
    ProductID: int
    ProductName: str
    Category: str
    Brand: str
    Quantity: int = Field(gt=0)
    UnitPrice: float = Field(gt=0)
    Discount: float = Field(ge=0)
    Tax: float = Field(ge=0)
    ShippingCost: float = Field(ge=0)
    TotalAmount: float = Field(gt=0)
    PaymentMethod: str
    OrderStatus: str
    City: str
    State: str
    Country: str
    SellerID: int

    @validator("Phone")
    def normalize_phone(cls, v):
        return ''.join(filter(str.isdigit, str(v)))

EXPECTED_COLUMNS = list(AmazonOrderSchema.__fields__.keys())

# ---------------- CORE LOGIC ----------------
def schema_validation_task():
    df = pd.read_csv(INPUT_PATH)

    # ---- Schema Drift Detection ----
    drift_report = {
        "missing_columns": [],
        "extra_columns": [],
        "auto_fixes": []
    }

    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None
            drift_report["missing_columns"].append(col)
            drift_report["auto_fixes"].append(f"Added missing column: {col}")

    for col in df.columns:
        if col not in EXPECTED_COLUMNS:
            drift_report["extra_columns"].append(col)

    df = df[EXPECTED_COLUMNS]

    # ---- Row Validation ----
    valid_rows = []
    row_errors = []

    for idx, row in df.iterrows():
        try:
            validated = AmazonOrderSchema(**row.to_dict())
            valid_rows.append(validated.dict())
        except Exception as e:
            row_errors.append({
                "row_number": idx,
                "error": str(e)
            })

    # ---- Save Outputs ----
    pd.DataFrame(valid_rows).to_csv(OUTPUT_DATA, index=False)

    with open(DRIFT_LOG, "w") as f:
        json.dump({
            "schema_drift": drift_report,
            "row_errors": row_errors
        }, f, indent=4)

    # ---- Schema Versioning ----
    version_entry = {
        "timestamp": datetime.now().isoformat(),
        "columns": EXPECTED_COLUMNS
    }

    try:
        with open(SCHEMA_VERSIONS, "r") as f:
            versions = json.load(f)
    except:
        versions = []

    versions.append(version_entry)

    with open(SCHEMA_VERSIONS, "w") as f:
        json.dump(versions, f, indent=4)

# ---------------- DAG DEFINITION ----------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="amazon_schema_validation_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Pydantic-based schema validation with drift detection"
) as dag:

    schema_validation = PythonOperator(
        task_id="run_schema_validation",
        python_callable=schema_validation_task
    )

    schema_validation
