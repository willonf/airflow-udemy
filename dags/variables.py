from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable


def print_variable():
    my_var = Variable.get("my_var")
    print(f"My variable: {my_var}")


with DAG(dag_id="variables", schedule_interval=None, start_date=datetime(2023, 4, 12), catchup=False, ) as variables:
    print_var_task = PythonOperator(task_id="print_var_task", python_callable=print_variable)

    print_var_task
