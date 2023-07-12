from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import statistics as st


def data_cleaner():
    df = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
    df.columns = [
        "id",
        "Score",
        "Estado",
        "Gênero",
        "Idade",
        "Patrimôno",
        "Saldo",
        "Produtos",
        "TemCartãoCrédito",
        "Ativo",
        "Salário",
        "Saiu",
    ]

    median = st.median(df["Salário"])
    df["Salário"].fillna(median, inplace=True)

    df["Gênero"].fillna("Masculino", inplace=True)

    median = st.median(df["Idade"])
    df.loc[(df["Idade"] < 0) | df["Idade"] > 120, "Idade"] = median

    df.drop_duplicates(subset="id", keep="first", inplace=True)

    df.to_csv("/opt/airflow/data/clean_churn.csv", sep=";", index=False)


with DAG(
    dag_id="python",
    schedule_interval=None,
    start_date=datetime(2023, 4, 12),
    catchup=False,
) as python:
    python_task = PythonOperator(task_id='python_task', python_callable=data_cleaner)
