from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vinayak',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': True,
    'email': ['test082001@yopmail.com'],
    'retries': 3,
    'retry_delay': timedelta(seconds=30),  # Delay between retries
}

with DAG(
    dag_id='retry_tasks',
    default_args=default_args,
    description='A sample DAG to demonstrate retries and retry delay',
    schedule_interval=None,  # You can set this to any cron expression
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'retry'],
) as dag:

    failing_task = BashOperator(
        task_id='failing_task',
        bash_command='exit 1',  # Simulating a failure
    )

    failing_task