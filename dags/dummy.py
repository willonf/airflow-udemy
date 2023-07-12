from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="dummy", schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dummy:
    task1 = BashOperator(task_id="task1", bash_command="echo TASK 1")
    task2 = BashOperator(task_id="task2", bash_command="echo TASK 2")
    task3 = BashOperator(task_id="task3", bash_command="echo TASK 3")
    task4 = BashOperator(task_id="task4", bash_command="echo TASK 4")
    task5 = BashOperator(task_id="task5", bash_command="echo TASK 5")

    task1 >> task2
