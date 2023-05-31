from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='trigger_dag_1',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as trigger_dag_1:

    task1 = BashOperator(task_id='task1', bash_command="echo TASK1")
    task2 = BashOperator(task_id='task2', bash_command="echo TASK2")
    task3 = BashOperator(task_id='task3', bash_command="echo TASK3", trigger_rule='one_failed')

    [task1,task2] >> task3

with DAG(dag_id='trigger_dag_2',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as trigger_dag_2:

    task1 = BashOperator(task_id='task1', bash_command="exit 1")
    task2 = BashOperator(task_id='task2', bash_command="echo TASK2")
    task3 = BashOperator(task_id='task3', bash_command="echo TASK3", trigger_rule='one_failed')

    [task1,task2] >> task3

with DAG(dag_id='trigger_dag_3',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as trigger_dag_3:

    task1 = BashOperator(task_id='task1', bash_command="echo TASK1")
    task2 = BashOperator(task_id='task2', bash_command="echo TASK2")
    task3 = BashOperator(task_id='task3', bash_command="echo TASK3", trigger_rule='all_failed')

    [task1,task2] >> task3

with DAG(dag_id='trigger_dag_4',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as trigger_dag_4:

    task1 = BashOperator(task_id='task1', bash_command="exit 1")
    task2 = BashOperator(task_id='task2', bash_command="exit 1")
    task3 = BashOperator(task_id='task3', bash_command="echo TASK3", trigger_rule='all_failed')

    [task1,task2] >> task3