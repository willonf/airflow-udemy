from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
import requests


def query_api():
    response = requests.get("https://api.publicapis.org/entries/")
    print(response.text)


with DAG(
    dag_id="http_sensor",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as http_sensor:
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="public_apis_api",  # Nome da conexão criada em Connections na interface gráfica
        endpoint="entries",
        poke_interval=5,
        timeout=20,
    )
    http_sensor_task = PythonOperator(
        task_id="http_sensor_task", python_callable=query_api
    )

    check_api >> http_sensor_task
