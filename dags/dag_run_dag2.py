from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_run_dag2",
    description="3 DAG criada",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as dag_run_dag2:
    task1 = BashOperator(task_id="task1", bash_command="echo TASK1")
    task2 = BashOperator(task_id="task2", bash_command="echo TASK2")

    task1 >> task2
