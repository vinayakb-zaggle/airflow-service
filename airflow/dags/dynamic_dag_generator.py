import os
import sys

# Add the airflow directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import DagRun
from airflow.utils.context import Context
import os

from utils.dag_generator import generate_pipeline_dag

def dynamic_dag_generator(**context: Context):
    conf = context["dag_run"].conf or {}

    pipeline_id = conf.get("pipelineId")
    connectors = conf.get("connectors")

    if not pipeline_id or not connectors:
        raise ValueError("Missing 'pipelineId' or 'connectors' in DAG conf")

    generate_pipeline_dag(pipeline_id, connectors)

with DAG(
    dag_id="create-pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dynamic", "dag-generator", "pipeline"]
) as dag:

    generate_dag_task = PythonOperator(
        task_id="generate_dag",
        python_callable=dynamic_dag_generator,
    )
