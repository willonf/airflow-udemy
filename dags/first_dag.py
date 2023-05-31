from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id='dag_1',description='1 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_1:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd", dag=dag_1)
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd", dag=dag_1)
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd", dag=dag_1)
    hello1 = BashOperator(task_id="hello1", bash_command="echo hello")

    task1 >> task2 >> task3 >> hello1


with DAG(dag_id='dag_2',description='2 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_2:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd", dag=dag_2)
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd", dag=dag_2)
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd", dag=dag_2)
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    [task1,task2,task3,hello]


with DAG(dag_id='dag_3',description='3 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_3:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd")
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd")
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd")
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    hello >> [task1,task2,task3]


with DAG(dag_id='dag_4',description='4 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_4:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd")
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd")
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd")
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    [task1,task2,task3] >> hello

with DAG(dag_id='dag_5',description='4 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_5:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd")
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd")
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd")
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    # Paralelizando com o método set:
    hello.set_downstream(task1)  # Tarefa anterior a hello
    hello.set_downstream(task2)  # Tarefa anterior a hello
    hello.set_downstream(task3)  # Tarefa anterior a hello


with DAG(dag_id='dag_6',description='4 DAG criada', schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False) as dag_6:

    task1 = BashOperator(task_id='task1', bash_command="echo $pwd")
    task2 = BashOperator(task_id='task2', bash_command="echo $pwd")
    task3 = BashOperator(task_id='task3', bash_command="echo $pwd")
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    # Serializando com o método set:
    hello.set_upstream(task1)  # Tarefa posterior a hello
    task1.set_upstream(task2)  # Tarefa posterior a task1
    task2.set_upstream(task3)  # Tarefa posterior a task2