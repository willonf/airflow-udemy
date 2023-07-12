from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table():
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    pg_hook.run("create table if not exists identifiers (id int);", autocommit=True)


def insert_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    pg_hook.run("insert into identifiers values (1);", autocommit=True)


def select_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres")
    records = pg_hook.get_records("select * from identifiers;")
    kwargs["ti"].xcom_push(key="query_records", value=records)


def show_result(**kwargs):
    task_instance = kwargs["ti"].xcom_pull(
        key="query_records", task_ids="select_data"
    )
    for result in task_instance:
        print(f"RESULTADO: {result}")


with DAG(
    dag_id="postgres_hook",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as postgres_hook:
    create_table = PythonOperator(task_id="create_table", python_callable=create_table)
    insert_data = PythonOperator(task_id="insert_data", python_callable=insert_data)
    select_data = PythonOperator(
        task_id="select_data", python_callable=select_data, provide_context=True
    )
    show_result = PythonOperator(
        task_id="show_result", python_callable=show_result, provide_context=True
    )

    create_table >> insert_data >> select_data >> show_result
