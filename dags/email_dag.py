from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 12),
    "email": ["willonf@icomp.ufam.edu.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="email_dag",
    description="default_args",
    schedule_interval=None,
    default_args=default_args,
    tags=["email"],
    catchup=False,
) as email_dag:
    # É possível sobrescrever o "retries" na task
    task1 = BashOperator(task_id="task1", bash_command="echo TASK1", retries=3)
    task2 = BashOperator(task_id="task2", bash_command="echo TASK2")
    task3 = BashOperator(task_id="task3", bash_command="echo TASK3")
    task4 = BashOperator(task_id="task4", bash_command="exit 1")

    email_task = EmailOperator(
        task_id="task_email",
        to="willonf@icomp.ufam.edu.br",
        subject="Airflow pipeline error",
        trigger_rule='one_failed',
        html_content="""<h3>Ocorreu um erro no pipeline da DAG 'email_dag'</h3>""",
    )

    task5 = BashOperator(
        task_id="task5", bash_command="echo TASK5", trigger_rule="none_failed"
    )
    task6 = BashOperator(
        task_id="task6", bash_command="echo TASK6", trigger_rule="none_failed"
    )

    [task1, task2] >> task3 >> task4 >> [email_task, task5, task6]
