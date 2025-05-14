import os
import sys
import json
from typing import Dict, Any

# Add the airflow directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator as DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from utils.connector_handler import process_webhook_request, execute_connector, create_update_pipeline_lifecycle

def get_pipeline_config():
    """
    Fetch pipeline configuration from Airflow Variables.
    Returns a dictionary mapping pipeline IDs to their steps.
    """
    try:
        config_json = Variable.get("pipeline_config", deserialize_json=True)
        if not config_json:
            raise ValueError("Pipeline configuration not found in Airflow Variables")
        return config_json
    except Exception as e:
        raise ValueError(f"Error fetching pipeline configuration: {str(e)}")

# Get config from Airflow Variables
config = get_pipeline_config()

# config = {
#    "pipeline1": ["step1", "step2"],
#    "pipeline2": ["step1", "step2", "step3", "step4"],
#    "pipeline3": ["step1", "step3", "step5", "step7"],
#    "4b4e2369-c997-4e8c-8429-309824f99122": ["EMS.expanse.report", "TSIP-NS.INVOICE"],
#    "3ffeb77a-e471-4a59-bbbf-7b27ce59afd5": ["EMS.expanse.report", "SQI.1db824c4-7135-4c14-a209-edfe86774b97", "D365.journal.line"]
# }

def webhook(**context):
    """
    Process webhook request and return pipeline ID
    """
    # Get the configuration from the DAG run context
    request_body = context['dag_run'].conf
    
    if not request_body:
        raise ValueError("No request body provided in DAG configuration")
    
    # Test failure condition - this will make the task fail
    if request_body.get('test_failure', False):
        raise Exception("Intentionally failing the webhook task for testing")
    
    connector_payload = process_webhook_request(request_body)
    
    context['task_instance'].xcom_push(key='connector_payload', value=connector_payload)
    return connector_payload['pipelineId']

def should_execute_task(ti, current_pipeline_id):
    """Helper function to determine if task should execute"""
    selected_pipeline = ti.xcom_pull(task_ids='webhook')
    return selected_pipeline == current_pipeline_id

def generic_task(step_name, pipeline_id):
    def _task(**context):
        ti = context['task_instance']
        if not should_execute_task(ti, pipeline_id):
            raise AirflowSkipException(f"Skipping {step_name} as it's not part of the selected pipeline")
        
        print(f"Running {step_name}")
        
        # Get connector payload from XCom
        connector_payload = ti.xcom_pull(task_ids='webhook', key='connector_payload')

        # Override is_final_connector based on step_name matching finalConnector
        is_final_connector = step_name == connector_payload['finalConnector']
        
        # Update database with status 0 before starting the task
        create_update_pipeline_lifecycle(
            pipeline_id=connector_payload['pipelineId'],
            client_id=connector_payload['clientId'], 
            tenant_id=connector_payload['tenantId'],
            connector=step_name,
            status=0,  # 0 indicates in progress
            request_payload=json.dumps(connector_payload.get('request', {})),
            response_payload="",  # Empty response payload as task hasn't completed yet
            trigger=connector_payload.get('trigger', 'webhook')
        )
        
        # Get previous step result if it exists
        if 'upstream_task_id' in context['params']:
            previous_result = ti.xcom_pull(task_ids=context['params']['upstream_task_id'])
            if previous_result:
                # Update connector payload with previous step's results
                if isinstance(previous_result, dict):
                    connector_payload.update(previous_result)
        
        # Execute the connector
        result = execute_connector(connector_payload, step_name, is_final_connector)
        
        # Store the updated connector_payload in XCom for the next task
        ti.xcom_push(key='connector_payload', value=result)
        return result
    return _task

def save_to_db_task(pipeline_id, step=None, **context):
    ti = context['task_instance']
    if not should_execute_task(ti, pipeline_id):
        raise AirflowSkipException(f"Skipping save_to_db as it's not part of the selected pipeline")
    
    # Get the result from the previous task
    upstream_task_id = context.get('params', {}).get('upstream_task_id')
    if not upstream_task_id:
        raise ValueError("save_to_db task requires an upstream task")
    
    # Get connector payload and previous result
    connector_payload = ti.xcom_pull(task_ids='webhook', key='connector_payload')
    previous_result = ti.xcom_pull(task_ids=upstream_task_id)
    
    if previous_result:
        # Update connector payload with previous step's results
        connector_payload.update(previous_result)
    
    # Determine if this is the final step
    is_final_connector = step == connector_payload['finalConnector']
    
    # Create/update pipeline lifecycle record with success status
    create_update_pipeline_lifecycle(
        pipeline_id=connector_payload['pipelineId'],
        client_id=connector_payload['clientId'], 
        tenant_id=connector_payload['tenantId'],
        connector=step,  # Using step as connector name
        status=1,  # 1 indicates success
        request_payload=json.dumps(connector_payload.get('request', {})),
        response_payload=json.dumps(connector_payload.get('response', {})),
        trigger=connector_payload.get('trigger', 'webhook')
    )

    # Return the previous result payload
    return previous_result if previous_result else connector_payload

# DAG definition
with DAG(
    dag_id="dynamic_steps_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['dynamic', 'steps']
) as dag:
    webhook = PythonOperator(
        task_id='webhook',
        python_callable=webhook
    )
    
    end = DummyOperator(task_id='end', trigger_rule='none_failed')
    
    # Create task groups for each pipeline
    for pipeline_id, steps in config.items():
        with TaskGroup(group_id=pipeline_id) as pipeline_group:
            previous_task = None
            save_to_db_counter = 1
            
            for step in steps:
                # Create connector task
                connector_task = PythonOperator(
                    task_id=step,
                    python_callable=generic_task(step, pipeline_id),
                    op_kwargs={
                        'pipeline_id': pipeline_id
                    },
                    params={
                        'upstream_task_id': previous_task.task_id if previous_task else None
                    }
                )
                
                if previous_task:
                    previous_task >> connector_task
                
                # Create save_to_db task after each connector
                save_task = PythonOperator(
                    # task_id=f"save_to_db_{save_to_db_counter}",
                    task_id=f"persist_{save_to_db_counter}",
                    python_callable=save_to_db_task,
                    op_kwargs={
                        'pipeline_id': pipeline_id,
                        'step': step
                    },
                    params={
                        'upstream_task_id': connector_task.task_id
                    }
                )
                
                connector_task >> save_task
                previous_task = save_task
                save_to_db_counter += 1
            
            # Connect the pipeline group to the main flow
            webhook >> pipeline_group >> end

