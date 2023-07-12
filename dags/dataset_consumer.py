from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

my_dataset = Dataset("/opt/airflow/data/churn2.csv")


def new_file():
    dataset = pd.read_csv("/opt/airflow/data/churn2.csv", sep=";")
    print(dataset.head(10))


with DAG(
    dag_id="dataset_consumer",
    schedule=[my_dataset],
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as dataset_consumer:
    consumer_task = PythonOperator(
        task_id="consumer_task", python_callable=new_file, provide_context=True
    )
