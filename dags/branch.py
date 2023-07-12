from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime
import random


def generate_random_number():
    return random.randint(0, 100)


def get_task(**kwargs):
    value = kwargs["ti"].xcom_pull(task_ids="random_number")
    print(f'VALUE: {value}')
    return "even_task" if value % 2 == 0 else "odd_task"


with DAG(
    dag_id="branch",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as branch:
    random_number = PythonOperator(
        task_id="random_number", python_callable=generate_random_number
    )
    branch_task = BranchPythonOperator(
        task_id="branch_task", python_callable=get_task, provide_context=True
    )
    even_task = BashOperator(task_id="even_task", bash_command="echo É PAR!")
    odd_task = BashOperator(task_id="odd_task", bash_command="echo É ÍMPAR!")

    random_number >> branch_task
    branch_task >> even_task
    branch_task >> odd_task
