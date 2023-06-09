from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(dag_id='dag_run_dag1', description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12),
         catchup=False) as dag_run_dag1:
    task1 = BashOperator(task_id='task1', bash_command="echo TASK1")
    task2 = TriggerDagRunOperator(task_id='task2', trigger_dag_id="dag_run_dag2", dag=dag_run_dag1)

task1 >> task2
