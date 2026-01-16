from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
import requests
import sqlite3
import os

BASE_PATH = "/opt/airflow/data"
INPUT_PATH = f"{BASE_PATH}/input"
OUTPUT_PATH = f"{BASE_PATH}/output"

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ---------------- CSV INGESTION ----------------
def ingest_csv():
    try:
        df = pd.read_csv(f"{INPUT_PATH}/amazon_data.csv")
        df.to_csv(f"{OUTPUT_PATH}/csv_data.csv", index=False)
        print("CSV ingestion successful")
    except Exception as e:
        print("CSV ingestion failed:", e)

# ---------------- JSON INGESTION ----------------
def ingest_json():
    try:
        df = pd.read_json(f"{INPUT_PATH}/orders.json")
        df.to_csv(f"{OUTPUT_PATH}/json_data.csv", index=False)
        print("JSON ingestion successful")
    except Exception as e:
        print("JSON ingestion failed:", e)

# ---------------- API INGESTION ----------------
def ingest_api():
    try:
        url = "https://jsonplaceholder.typicode.com/posts"
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(f"{OUTPUT_PATH}/api_data.csv", index=False)
        print("API ingestion successful")
    except Exception as e:
        print("API ingestion failed:", e)

# ---------------- SQL INGESTION ----------------
def ingest_sql():
    try:
        conn = sqlite3.connect(f"{BASE_PATH}/orders.db")
        df = pd.read_sql("SELECT * FROM orders", conn)
        df.to_csv(f"{OUTPUT_PATH}/sql_data.csv", index=False)
        conn.close()
        print("SQL ingestion successful")
    except Exception as e:
        print("SQL ingestion failed:", e)

# ---------------- FILE WATCHER ----------------
def file_watcher():
    files = os.listdir(INPUT_PATH)
    if not files:
        raise FileNotFoundError("No input files found")
    print("Files detected:", files)

# ---------------- DAG ----------------
with DAG(
    dag_id="multi_source_data_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    watch_files = PythonOperator(
        task_id="watch_input_files",
        python_callable=file_watcher
    )

    csv_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv
    )

    json_task = PythonOperator(
        task_id="ingest_json",
        python_callable=ingest_json
    )

    api_task = PythonOperator(
        task_id="ingest_api",
        python_callable=ingest_api
    )

    sql_task = PythonOperator(
        task_id="ingest_sql",
        python_callable=ingest_sql
    )

    watch_files >> [csv_task, json_task, api_task, sql_task]
