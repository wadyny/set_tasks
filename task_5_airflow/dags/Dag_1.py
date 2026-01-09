import pandas as pd
import os
import re
from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

BASE_DIR = os.getenv("BASE_DIR", "dataset")
IN_FILE = os.path.join(BASE_DIR, os.getenv("IN_FILE"))
WORKING_FILE = os.path.join(BASE_DIR, os.getenv("WORKING_FILE"))
FINAL_FILE = os.path.join(BASE_DIR, os.getenv("FINAL_FILE"))
PROCESSED_DATASET = Dataset(FINAL_FILE)

def check_file_empty(**context):
    try:
        df = pd.read_csv(IN_FILE)
        if df.empty:
            return "log_empty_file"
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return "log_empty_file"
    return "transformations.clean_content"

def replace_nulls(**context):
    df = pd.read_csv(WORKING_FILE, dtype=str)
    df = df.fillna("-")
    df = df.replace(to_replace=r"^\s*$", value="-", regex=True)
    df.to_csv(FINAL_FILE, index=False)


def sort_by_created_date(**context):
    df = pd.read_csv(WORKING_FILE)
    if "created_date" in df:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        df = df.sort_values("created_date")
    df.to_csv(WORKING_FILE, index=False)


def clean_content(**context):

    df = pd.read_csv(IN_FILE, dtype=str)
    def clean_text(s):
        if pd.isna(s):
            return s
        text = str(s)
        cleaned = re.sub(r"[^0-9A-Za-zА-Яа-яёЁ\s\.\,\!\?\:\;\-\"\'\(\)]", "", text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    if "content" in df.columns:
        df["content"] = df["content"].apply(clean_text)

    df.to_csv(WORKING_FILE, index=False)

with DAG(
    dag_id="Dag_1",
    start_date=datetime(2025, 11, 10),
    schedule=None,
    catchup=False,
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_csv_file",
        filepath=IN_FILE,
        poke_interval=30,
        timeout=3600,
        mode="poke",
    )

    branch = BranchPythonOperator(
        task_id="branch_check_file",
        python_callable=check_file_empty,
    )

    log_empty = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "File {IN_FILE} is empty or missing at $(date)"',
    )

    with TaskGroup("transformations") as transformations:
        t1 = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
        )
        t2 = PythonOperator(
            task_id="sort_by_created_date",
            python_callable=sort_by_created_date,
        )
        t3 = PythonOperator(
            task_id="replace_nulls",
            python_callable=replace_nulls,
        )

        t1 >> t2 >> t3

    publish_dataset = BashOperator(
        task_id="publish_processed_data",
        bash_command=f"echo 'Processed file is ready at {FINAL_FILE}'",
        outlets=[PROCESSED_DATASET],
    )

    wait_for_file >> branch
    branch >> log_empty
    branch >> transformations >> publish_dataset
