from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 12),
    "email": ["contatowillon@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="default_args",
    description="default_args",
    schedule_interval="@hourly",
    start_date=datetime(2023, 4, 12),
    default_args=default_args,
    tags=["processo", "tag", "pipeline"],
    catchup=False,
) as dag_run_dag1:
    # É possível sobrescrever o "retries" na task
    task1 = BashOperator(task_id="task1", bash_command="echo TASK1", retries=3)
    task2 = BashOperator(task_id="task2", bash_command="echo TASK2")
    task3 = BashOperator(task_id="task3", bash_command="echo TASK3")

    task1 >> task2 >> task3
