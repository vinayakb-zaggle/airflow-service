import os
import sys
import json
from typing import Dict, Any

# Add the airflow directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.connector_utils import call_java_transform

def call_java_transform_task(**kwargs):
    # Prepare the fields for transformation
    # fields = [{
    #     "targetField": "name",
    #     "sourceField": ["name"],
    #     "transformationScope": [],
    #     "dataType": {}
    # }]

    # {sourceField=[EMS_expanse_report.data[0].data.trip_id, EMS_expanse_report.data[0].employeeUser.employeeId], transformationScope=[], targetField=[@].memo, dataType={}}, {sourceField=[EMS_expanse_report.data[0].data.total_report_amount], transformationScope=[], targetField=[@].account[*].amount, dataType={}}]

    fields = [
        {
            "targetField": "[@].transDate",
            "sourceField": ["EMS_expanse_report.data[0].statusUpdatedAt"],
            "transformationScope": [],
            "dataType": {}
        },
        {
            "targetField": "[@].memo",
            "sourceField": ["EMS_expanse_report.data[0].employeeUser.employeeId"],
            "transformationScope": [],
            "dataType": {}
        },
        {
            "targetField": "[@].account[*].amount",
            "sourceField": ["EMS_expanse_report.data[0].data.total_report_amount"],
            "transformationScope": [],
            "dataType": {}
        }
    ]

    print("Fields:", fields)

    # Prepare the source payload as a JSON string
    
    source_payload = '[{"EMS_expanse_report":{"data":[{"id":"49a6c10e-c6e4-4c24-a258-df5319cb7f1f","data":{"trip_id":"","is_mobile":false,"total_report_amount":550.0,"travel_expense_report":"","limit_exceeded_message":"","connected_list_option_id":null},"statusUpdatedAt":"23/01/2025","employeeUser":{"id":"3f9273f3-db4f-4f3d-a9c8-882a45fa3d6c","employeeId":"COO11"}}]}}]'
    # Prepare the source payload
    # source_payload = '{"name":"Alice", "email":"alice@example.com"}'

    # Call the transformation method
    result = call_java_transform(fields, source_payload)
    print("Java Result:", result)

# Define the DAG
with DAG(
    dag_id="java_transformer",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["java-transformer"],
) as dag:

    run_transform = PythonOperator(
        task_id="run_java_transform",
        python_callable=call_java_transform_task
    )