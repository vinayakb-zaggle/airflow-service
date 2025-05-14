from typing import Dict, Any, Optional, List
from airflow.providers.mongo.hooks.mongo import MongoHook
import requests
import json
import uuid
from datetime import datetime, timezone
import jpype
import jpype.imports
from jpype.types import JString

import logging
import requests
from typing import Dict, Any

import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from functools import lru_cache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_webhook_request(request_body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process the webhook request and prepare connector payload
    """
    # Get the collections
    webhook_collection = get_collection('webhook')
    pipeline_collection = get_collection('pipeline')
    
    # Find webhook data
    webhook_data = webhook_collection.find_one({
        'clientId': request_body.get('clientId'),
        'tenantId': request_body.get('tenantId'),
        'finalConnector': request_body.get('finalConnector')
    })
    
    if not webhook_data:
        raise ValueError("Webhook not found")
    
    # Find pipeline data
    pipeline_data = pipeline_collection.find_one({
        '_id': webhook_data['pipelineId'],
        'isActive': True
    })
    
    if not pipeline_data:
        raise ValueError("Pipeline not found")
    
    # Create connector payload
    connector_payload = {
        'clientId': webhook_data['clientId'],
        'tenantId': webhook_data['tenantId'],
        'pipelineId': webhook_data['pipelineId'],
        'finalConnector': webhook_data['finalConnector'],
        'finalConnectorMappings': pipeline_data.get('mappings', {}),
        'defaultValues': json.loads(webhook_data.get('defaultValues', '{}')),
        'trigger': 'webhook',
        'connectors': pipeline_data.get('response', []),
        'initialPayload': request_body.get('payload'),
        'finalRequestPayload': {},
        'request': request_body.get('payload'),
        'response': {}
    }
    
    # Create final request payload
    for request_connector in pipeline_data.get('request', []):
        connector_payload['finalRequestPayload'][request_connector.replace('.', '_')] = request_body.get('payload')
    
    return connector_payload

def execute_connector(connector_payload: Dict[str, Any], connector: str, is_final_connector: bool) -> Dict[str, Any]:
    """
    Execute the connector based on the webhook payload
    
    Args:
        connector_payload (Dict[str, Any]): The connector payload containing necessary information
        connector (str): The name of the connector to execute
        is_final_connector (bool): Flag indicating if this is the final connector in the pipeline
    """
    # Extract required parameters
    client_id = connector_payload['clientId']
    tenant_id = connector_payload['tenantId']
    pipeline_id = connector_payload['pipelineId']
    
    payload = connector_payload.get('finalRequestPayload', {}) if is_final_connector else connector_payload.get('initialPayload', {})
    
    default_values = connector_payload.get('defaultValues', {}) if is_final_connector else {}
    trigger = connector_payload.get('trigger')

    # Add connector name to the payload
    connector_payload['connector'] = connector

    create_update_pipeline_lifecycle(
        pipeline_id=connector_payload['pipelineId'],
        client_id=connector_payload['clientId'], 
        tenant_id=connector_payload['tenantId'],
        connector=connector,
        status=0,  # 0 indicates in progress
        request_payload=json.dumps(connector_payload.get('request', {})),
        response_payload="",  # Empty response payload as task hasn't completed yet
        trigger=connector_payload.get('trigger', 'webhook')
    )
    
    if "SQI" in connector:
        # Initialize MongoDB connection
        collection = get_collection("sequence_invocations")
        # Handle Sequence Invocation case
        sequence_invocation_id = connector.split(".")[1]
        
        # Fetch sequence invocation from MongoDB
        sequence_invocation = collection.find_one({'_id': sequence_invocation_id})
        
        if not sequence_invocation:
            raise Exception("Sequence Invocation not found")
        
        sqi_source_connector = sequence_invocation['source']
        
        # Execute source connector
        if f"{sqi_source_connector.replace('.', '_')}" not in connector_payload.get('finalRequestPayload', {}):
            # Make API call for source connector
            source_response = execute_api_call(
                pipeline_id=pipeline_id,
                connector=sqi_source_connector,
                client_id=client_id,
                tenant_id=tenant_id,
                payload=payload
            )
            
            if source_response.get('status') == 'FAILURE':
                create_update_pipeline_lifecycle(
                    pipeline_id=pipeline_id,
                    client_id=client_id,
                    tenant_id=tenant_id,
                    connector=sqi_source_connector,
                    status=2,  # 2 represents failure
                    request_payload=json.dumps(payload),
                    response_payload=json.dumps(source_response.get('data')),
                    trigger=trigger
                )
                raise Exception("Sequence Invocation Failed at Source")
            
            connector_payload['finalRequestPayload'][sqi_source_connector.replace('.', '_')] = source_response.get('data')
        
        # Prepare and execute destination connector
        sqi_destination_connector = sequence_invocation['destination']
        destination_payload = connector_payload['finalRequestPayload'][sqi_source_connector.replace('.', '_')]
        
        # Get mappings if they exist
        mappings = sequence_invocation.get('mappings')
        
        if mappings:
            transformed_payload = call_java_transform(
                fields=mappings,
                source_payload=json.dumps(destination_payload)
            )
            destination_payload = json.loads(transformed_payload)
        
        # Update the request field in connector payload with transformed payload
        connector_payload['request'] = destination_payload

        destination_response = execute_api_call(
            pipeline_id=pipeline_id,
            connector=sqi_destination_connector,
            client_id=client_id,
            tenant_id=tenant_id,
            payload=destination_payload
        )
        
        if destination_response.get('status') == 'FAILURE':
            create_update_pipeline_lifecycle(
                pipeline_id=pipeline_id,
                client_id=client_id,
                tenant_id=tenant_id,
                connector=sqi_destination_connector,
                status=2,  # 2 represents failure
                request_payload=json.dumps(destination_payload),
                response_payload=json.dumps(destination_response.get('data')),
                trigger=trigger
            )
            raise Exception("Sequence Invocation Failed at Destination")
        
        # Update the response field in connector payload with the response data
        connector_payload['response'] = destination_response.get('data', {}).get('data', destination_response.get('data', {}))
        connector_payload['finalRequestPayload'][sqi_destination_connector.replace('.', '_')] = connector_payload['response']
        
    else:
        # Handle regular connector case
        mappings = None
        if connector_payload.get('finalConnectorMappings') and is_final_connector:
            mappings = connector_payload['finalConnectorMappings']
        
        # Transform the payload using Java transformer if mappings exist
        if mappings:
            transformed_payload = call_java_transform(
                fields=mappings,
                source_payload=json.dumps(payload)
            )
            payload = json.loads(transformed_payload)
        
        # Update the request field in connector payload with transformed payload
        connector_payload['request'] = payload

        response = execute_api_call(
            pipeline_id=pipeline_id,
            connector=connector,
            client_id=client_id,
            tenant_id=tenant_id,
            payload=payload
        )

        logger.info(f"Api Response: {response}")
        
        if response.get('status') == 'FAILURE':
            create_update_pipeline_lifecycle(
                pipeline_id=pipeline_id,
                client_id=client_id,
                tenant_id=tenant_id,
                connector=connector,
                status=2,  # 2 represents failure
                request_payload=json.dumps(payload),
                response_payload=json.dumps(response.get('data')),
                trigger=trigger
            )
            raise Exception(f"Execution failed for connector: {connector}")
        
        # Update the response field in connector payload with the response data
        connector_payload['response'] = response.get('data')
        
        if f"{connector.replace('.', '_')}" not in connector_payload.get('finalRequestPayload', {}):
            connector_payload['finalRequestPayload'][connector.replace('.', '_')] = response.get('data')
    logger.info(f"Operation Completes for: {connector}")
    return connector_payload

def create_update_pipeline_lifecycle(
    pipeline_id: str,
    client_id: str,
    tenant_id: str,
    connector: str,
    status: int,
    request_payload: str,
    response_payload: str,
    trigger: str
) -> None:
    """
    Create or update pipeline lifecycle record in MongoDB
    
    Args:
        pipeline_id (str): The pipeline ID
        client_id (str): The client ID
        tenant_id (str): The tenant ID
        connector (str): The connector name
        status (int): The status of the pipeline (0: in progress, 1: success, 2: failure)
        request_payload (str): The request payload as JSON string
        response_payload (str): The response payload as JSON string
        trigger (str): The trigger type
    """
    collection = get_collection('pipelineLifecycle')
    
    # Find existing record
    existing = collection.find_one({
        'pipelineId': pipeline_id,
        'connector': connector
    })
    
    # Prepare document
    now = datetime.now(timezone.utc)
    doc = {
        'pipelineId': pipeline_id,
        'clientId': client_id,
        'tenantId': tenant_id,
        'connector': connector,
        'status': status,
        'trigger': trigger,
        'requestPayload': request_payload,
        'responsePayload': response_payload,
        'updatedAt': now
    }
    
    if existing:
        # Update existing record
        collection.update_one(
            {'_id': existing['_id']},
            {'$set': doc}
        )
    else:
        # Create new record
        doc['_id'] = str(uuid.uuid4())
        doc['createdAt'] = now
        collection.insert_one(doc)
    logger.info(f"Details Saved Successfully!")

def call_java_transform(
    fields: List[Dict[str, Any]],
    source_payload: str,
    jar_path: str = "/opt/airflow/libs/transformer-lib-1.0.0-SNAPSHOT.jar",
    java_class: str = "org.zaggle.transformer.TransformUtilityOpenConnector"
) -> str:
    """
    Call Java transformation utility to transform data based on field mappings.
    
    Args:
        fields (List[Dict[str, Any]]): List of field mappings containing source and target field information
        source_payload (str): JSON string containing the source data to transform
        jar_path (str): Path to the Java JAR file
        java_class (str): Fully qualified name of the Java class to use
        
    Returns:
        str: Transformed data as a string
    """
    try:
        # Start JVM if not already started
        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=[jar_path])

        # Import Java classes
        HashMap = jpype.JClass("java.util.HashMap")
        ArrayList = jpype.JClass("java.util.ArrayList")
        ObjectMapper = jpype.JClass("com.fasterxml.jackson.databind.ObjectMapper")
        Transformer = jpype.JClass(java_class)

        # Convert Python fields to Java List<Map<String, Object>>
        java_fields = ArrayList()
        for field in fields:
            map1 = HashMap()
            map1.put("targetField", field.get("targetField", ""))
            
            source_fields = ArrayList()
            for source_field in field.get("sourceField", []):
                source_fields.add(source_field)
            map1.put("sourceField", source_fields)
            
            map1.put("transformationScope", ArrayList())
            map1.put("dataType", HashMap())
            
            java_fields.add(map1)
        
        logger.info(f"Field Mapping: {java_fields}")
        # Prepare the JSON payload as a JsonNode
        mapper = ObjectMapper()
        payload = mapper.readTree(JString(source_payload))

        logger.info(f"Source Payload: {payload}")

        # Create transformer instance and call the method
        transformer_instance = Transformer()
        result = transformer_instance.transformData(java_fields, payload)

        logger.info(f"Transformed Payload: {result}")
        
        # Convert Java String to Python string
        return str(result)
    except Exception as e:
        # Log the error but don't shut down JVM
        print(f"Error in Java transformation: {str(e)}")
        raise 

def execute_api_call(
    pipeline_id: str,
    connector: str,
    client_id: str,
    tenant_id: str,
    payload: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Execute API call to the connector endpoint
    
    Args:
        pipeline_id (str): The pipeline ID
        connector (str): The connector name
        client_id (str): The client ID
        tenant_id (str): The tenant ID
        payload (Dict[str, Any]): The payload to send
        
    Returns:
        Dict[str, Any]: Response from the API call
    """
    if connector == 'EMS.expanse.report':
        try:
            # Construct the API endpoint URL
            api_url = f"https://dev-zig-svc.zaggle.in/api/v1/zig/airflow/execute/api"
            
            # Prepare the request payload
            request_payload = {
                "pipelineId": pipeline_id,
                "connector": connector,
                "clientId": client_id,
                "tenantId": tenant_id,
                "requestBody": payload
            }
            
            # Make the API call
            response = requests.post(
                api_url,
                json=request_payload,
                headers={"Content-Type": "application/json"}
            )
            
            # Get the response data
            response_data = response.json()
            
            # Check if status is Failed
            if response_data.get('status') == 'Failure':
                return {
                    "status": "FAILURE",
                    "data": response_data
                }
            
            # Extract only the data field if it exists
            if response_data.get('data'):
                return {
                    "status": "SUCCESS",
                    "data": response_data['data']
                }
            else:
                return {
                    "status": "SUCCESS",
                    "data": response_data
                }
            
        except requests.exceptions.RequestException as e:
            return {
                "status": "FAILURE",
                "data": {
                    "error": str(e)
                }
            }
        except Exception as e:
            return {
                "status": "FAILURE",
                "data": {
                    "error": f"Unexpected error: {str(e)}"
                }
            }
    # Default dummy response for other connectors need to remove this.
    return {
        "status": "SUCCESS",
        "data": {
            "message": "Dummy success response",
            "connector": connector,
            "pipelineId": pipeline_id,
            "clientId": client_id,
            "tenantId": tenant_id
        }
    }

@lru_cache(maxsize=1)
def get_mongo_client() :
    mongo_uri = os.getenv("ZIG_MONGO_DB_URI")
    if not mongo_uri:
        raise ValueError("ZIG_MONGO_DB_URI is not set in the environment variable")
    client = MongoClient(mongo_uri)
    logging.info(f"Get mongo client called")
    try:
        client.admin.command('ping')
    except ConnectionFailure as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        raise

    return client

def get_mongo_db(client):
    # Get database name from environment variables
    db_name = os.getenv("ZIG_MONGO_DB")
    if not db_name:
        raise ValueError("MongoDB database name is not set in the environment variables")
    db = client[db_name]

    try:
        db.list_collection_names()
    except Exception as e:
        logging.error(f"Error accessing the database: {e}")
        raise

    return db

def get_collection(name: str):
    client = get_mongo_client()
    db = get_mongo_db(client)
    return db[name]