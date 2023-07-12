from airflow import DAG
from datetime import datetime
from plugins.big_data_operator import BigDataOperator

with DAG(
    dag_id="operator",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as operator:
    big_data_task = BigDataOperator(
        task_id="big_data_task",
        path_to_csv="/opt/airflow/data/Churn.csv",
        path_to_save_file="/opt/airflow/data/Churn.json",
        file_type="json",
    )
