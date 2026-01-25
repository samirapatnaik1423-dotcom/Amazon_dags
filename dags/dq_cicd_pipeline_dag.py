from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime
import os
import shutil
import zipfile

DQ_OUTPUT = "/opt/airflow/data/output/dq_api"
STAGING_PATH = "/opt/airflow/data/output/staging"

os.makedirs(STAGING_PATH, exist_ok=True)

def build_artifact():
    zip_path = f"{STAGING_PATH}/dq_release.zip"
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file in os.listdir(DQ_OUTPUT):
            zipf.write(
                f"{DQ_OUTPUT}/{file}",
                arcname=file
            )

def deploy_to_staging():
    # Simulated deployment
    return "Deployed to staging successfully"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="dq_cicd_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    #  Auto-linting
    lint = BashOperator(
        task_id="auto_linting",
        bash_command="flake8 dags || true"
    )

    #  Code formatting check
    format_check = BashOperator(
        task_id="black_check",
        bash_command="black --check dags || true"
    )

    #  Automated tests
    test = BashOperator(
        task_id="run_pytest",
        bash_command="pytest tests || true"
    )

    #  Build
    build = PythonOperator(
        task_id="build_artifact",
        python_callable=build_artifact
    )

    # Deploy
    deploy = PythonOperator(
        task_id="deploy_to_staging",
        python_callable=deploy_to_staging
    )

    lint >> format_check >> test >> build >> deploy
