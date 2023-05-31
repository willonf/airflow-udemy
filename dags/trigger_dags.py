from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='trigger_dag_1',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as trigger_dag_1:

    task1 = BashOperator(task_id='task1', bash_command="echo TASK1")
    task2 = BashOperator(task_id='task2', bash_command="echo TASK2")
    task3 = BashOperator(task_id='task3', bash_command="echo TASK3", trigger_rule='one_failed')

    [task1,task2] >> task3