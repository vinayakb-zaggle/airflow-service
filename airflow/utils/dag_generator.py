# plugins/custom_dag_generator/generator.py
import os
import boto3
from urllib.parse import urlparse
from jinja2 import Template
from airflow.exceptions import AirflowSkipException

DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
TEMPLATE_PATH = "/opt/airflow/templates/dag_template.py.j2"
s3_path = os.environ.get("AIRFLOW_REMOTE_DAGS_FOLDER")

if not s3_path:
    raise ValueError("Environment variable 'AIRFLOW_REMOTE_DAGS_FOLDER' is not set")

parsed_s3 = urlparse(s3_path)
s3_bucket = parsed_s3.netloc
s3_prefix = parsed_s3.path.lstrip("/")

def generate_pipeline_dag(pipeline_id: str, connectors: list):
    tasks = [
        {
            "variable_name": f"task{i+1}",
            "persist_variable_name": f"persist_{i+1}",
            "task_id": connector
        }
        for i, connector in enumerate(connectors)
    ]

    with open(TEMPLATE_PATH, "r") as file:
        template = Template(file.read())

    rendered_dag = template.render(
        dag_id=pipeline_id,
        tasks=tasks
    )

    dag_filename = f"{pipeline_id}.py"
    s3_key = f"{s3_prefix}{dag_filename}"
    local_dag_path = os.path.join(DAGS_FOLDER, dag_filename)

    s3 = boto3.client("s3")
    should_upload_to_s3 = True
    try:
        existing_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        existing_content = existing_obj["Body"].read().decode("utf-8")
        if existing_content == rendered_dag:
            should_upload_to_s3 = False
            print(f"Skipping S3 upload: DAG '{pipeline_id}' is already up to date.")
        else:
            print(f"Updating DAG '{pipeline_id}' in S3...")
    except s3.exceptions.NoSuchKey:
        print(f"Creating new DAG '{pipeline_id}' in S3...")

    if should_upload_to_s3:
        s3.put_object(Body=rendered_dag, Bucket=s3_bucket, Key=s3_key)
        print(f"DAG uploaded to s3://{s3_bucket}/{s3_key}")

    if os.path.exists(local_dag_path):
        with open(local_dag_path, "r") as f:
            existing_local = f.read()
        if existing_local == rendered_dag:
            print(f"Skipping local write: DAG '{pipeline_id}' is already up to date.")
            return
        else:
            print(f"Updating local DAG file '{local_dag_path}'...")
    else:
        print(f"Creating local DAG file '{local_dag_path}'...")

    with open(local_dag_path, "w") as local_file:
        local_file.write(rendered_dag)
    print(f"DAG written locally to {local_dag_path}")
