from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import pendulum
import pandas as pd
import json
import os

# ---------------- TIMEZONE ----------------
local_tz = pendulum.timezone("Asia/Kolkata")

# ---------------- PATHS ----------------
INPUT_PATH = "/opt/airflow/data/input/amazon_data.csv"

OUTPUT_BASE = "/opt/airflow/data/output/sprint_5"

HOURLY_PATH = f"{OUTPUT_BASE}/hourly/amazon_hourly.csv"
DAILY_PATH = f"{OUTPUT_BASE}/daily/amazon_daily.csv"
WEEKLY_PATH = f"{OUTPUT_BASE}/weekly/amazon_weekly.csv"
META_PATH = f"{OUTPUT_BASE}/metadata_log.json"

# Create folders dynamically
for layer in ["hourly", "daily", "weekly"]:
    os.makedirs(f"{OUTPUT_BASE}/{layer}", exist_ok=True)

# ---------------- SLA CALLBACK ----------------
def sla_miss_callback(dag, task_list, *args):
    print("❌ SLA MISSED for tasks:", task_list)

# ---------------- CORE LOGIC ----------------
def run_pipeline(**context):
    execution_date = context["execution_date"].in_timezone(local_tz)

    df = pd.read_csv(INPUT_PATH)
    df.columns = df.columns.str.strip()

    metadata = {
        "execution_time": execution_date.isoformat(),
        "records": len(df),
        "pipelines_run": [],
        "status": "SUCCESS"
    }

    # -------- HOURLY --------
    df.to_csv(HOURLY_PATH, index=False)
    metadata["pipelines_run"].append("hourly")

    # -------- DAILY (02:00 IST) --------
    if execution_date.hour == 2:
        df.to_csv(DAILY_PATH, index=False)
        metadata["pipelines_run"].append("daily")

    # -------- WEEKLY (MONDAY 03:00 IST) --------
    if execution_date.day_of_week == 0 and execution_date.hour == 3:
        df.to_csv(WEEKLY_PATH, index=False)
        metadata["pipelines_run"].append("weekly")

    with open(META_PATH, "w") as f:
        json.dump(metadata, f, indent=4)

    print("Pipelines executed:", metadata["pipelines_run"])

# ---------------- DAG ----------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="amazon_merged_schedule_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    schedule_interval="0 * * * *",  # HOURLY
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    sla_miss_callback=sla_miss_callback,
    description="Merged Hourly Daily Weekly Amazon Pipeline (Sprint 5)"
) as dag:

    run_all = PythonOperator(
        task_id="run_hourly_daily_weekly_pipeline",
        python_callable=run_pipeline,
        provide_context=True,
        sla=timedelta(minutes=15)
    )

    run_all
