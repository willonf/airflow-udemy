from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="complex_dag", description="3 DAG criada", schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as complex_dag:
    task1 = BashOperator(task_id="task1", bash_command="echo TASK1")
    task2 = BashOperator(task_id="task2", bash_command="echo TASK2")
    task3 = BashOperator(task_id="task3", bash_command="echo TASK3")
    task4 = BashOperator(task_id="task4", bash_command="echo TASK4")
    task5 = BashOperator(task_id="task5", bash_command="echo TASK5")
    task6 = BashOperator(task_id="task6", bash_command="echo TASK6")
    task7 = BashOperator(task_id="task7", bash_command="echo TASK7")
    task8 = BashOperator(task_id="task8", bash_command="echo TASK8")
    task9 = BashOperator(task_id="task9", bash_command="echo TASK9")

    # [task1 >> task2, task3 >> task4] >> task5 >> task6 >> [task7, task8, task9]
    # OU
    task1 >> task2,
    task3 >> task4,
    [task2, task4] >> task5 >> task6,
    task6 >> [task7, task8, task9]
