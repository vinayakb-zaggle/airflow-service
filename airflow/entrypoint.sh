#!/bin/bash

set -e  # Exit immediately on failure

echo "==> Syncing DAGs from S3..."
aws s3 sync $AIRFLOW_REMOTE_DAGS_FOLDER /opt/airflow/dags/ --exact-timestamps

echo "==> Starting Airflow with command: $@"
exec "$@"
