from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id="pool", schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False, ) as pool:
    task1 = BashOperator(task_id="task1", bash_command='sleep 5', pool='unique_slot_pool')
    task2 = BashOperator(task_id="task2", bash_command='sleep 5', pool='unique_slot_pool', priority_weight=5)
    task3 = BashOperator(task_id="task3", bash_command='sleep 5', pool='unique_slot_pool')
    task4 = BashOperator(task_id="task4", bash_command='sleep 5', pool='unique_slot_pool', priority_weight=10)

    task1, task2, task3, task4
