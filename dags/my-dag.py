from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hi(who):
    return f'Hi {who}'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'my_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 7),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='first_task',
        python_callable=say_hi,
        op_kwargs={'who': 'Carol'},
    )

    task2 = PythonOperator(
        task_id='second_task',
        python_callable=say_hi,
        op_kwargs={'who': 'Back at you'},
    )

    task1 >> task2
