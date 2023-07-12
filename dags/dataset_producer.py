from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

my_dataset = Dataset("/opt/airflow/data/clean_churn.csv")


def my_file():
    dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    dataset.to_csv("/opt/airflow/data/churn2.csv", sep=";")


with DAG(
    dag_id="dataset_producer",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as dataset_producer:
    producer_task = PythonOperator(
        task_id="producer_task", python_callable=my_file, outlets=[my_dataset]
    )
