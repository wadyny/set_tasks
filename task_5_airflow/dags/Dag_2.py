from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from pymongo import MongoClient
import pandas as pd
import os

BASE_DIR = os.getenv("BASE_DIR", "dataset")
FINAL_FILE = os.path.join(BASE_DIR, os.getenv("FINAL_FILE"))
PROCESSED_DATASET = Dataset(FINAL_FILE)


def read_and_load_to_mongo(**kwargs):

    df = pd.read_csv(FINAL_FILE)
    all_records = df.to_dict('records')

    client = MongoClient("mongodb://user:pass@mongodb:27017/")
    db = client['tiktok_db']
    collection = db['videos']

    collection.delete_many({})
    collection.insert_many(all_records)
    print(f"Successfully loaded {len(all_records)} records into MongoDB.")


with DAG(
        dag_id="Dag_2",
        start_date=datetime(2025, 11, 10),
        schedule=[PROCESSED_DATASET],
        catchup=False,
) as dag_mongo:
    load_to_mongo_task = PythonOperator(
        task_id="load_processed_data_to_mongo",
        python_callable=read_and_load_to_mongo,
    )

    mongo_success = BashOperator(
        task_id="log_mongo_success",
        bash_command="echo 'Data successfully loaded into MongoDB!'",
    )

    load_to_mongo_task >> mongo_success