from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator


def show_result(**kwargs):
    task_instance = kwargs['ti'].xcom_pull(task_ids="query_data")
    for task in task_instance:
        print(f"RESULTADO: {task}")


with DAG(
    dag_id="postgres_operator",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as postgres_operator:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql="create table if not exists test(id int);",
    )
    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres",
        sql="insert into test values (1);",
    )
    query_data = PostgresOperator(
        task_id="query_data",
        postgres_conn_id="postgres",
        sql="select * from test;",
    )
    show_result = PythonOperator(
        task_id="show_result", python_callable=show_result, provide_context=True
    )

    create_table >> insert_data >> query_data >> show_result
