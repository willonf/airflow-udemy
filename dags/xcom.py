from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_write(**kwargs):
    kwargs['ti'].xcom_push(key='valor_xcom1', value=42)

def task_read(**kwargs):
    value = kwargs['ti'].xcom_pull(key='valor_xcom1')
    print(f'Value: {value}')

with DAG(dag_id='xcom_dag', description='3 DAG criada',
         schedule_interval=None, start_date=datetime(2023, 4, 12),
         catchup=False) as xcom_dag:
    
    task1 = PythonOperator(task_id='task1', python_callable=task_write)
    task2 = PythonOperator(task_id='task2',  python_callable=task_read)

    task1 >> task2
