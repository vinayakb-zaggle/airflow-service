from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import io

# Define your S3 bucket and file details
# S3_BUCKET_NAME = "bucket-vinayak"
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
# S3_FILE_KEY = "customers.csv"
S3_FILE_KEY = Variable.get("S3_FILE_KEY")

# Define the DAG
dag = DAG(
    's3_to_postgres_dag',
    default_args={'start_date': days_ago(1),'owner': 'Vinayak',},
    schedule_interval='*/3 * * * *',  # Runs every 3 minutes
    catchup=False,
    tags=['s3', 'postgres'],
)

def fetch_s3_file():
    """Fetch CSV file from S3 and return it as a Pandas DataFrame"""
    s3_hook = S3Hook(aws_conn_id="my_aws_s3")
    file_obj = s3_hook.get_key(S3_FILE_KEY, S3_BUCKET_NAME)
    
    if not file_obj:
        raise ValueError(f"File {S3_FILE_KEY} not found in bucket {S3_BUCKET_NAME}")

    file_content = file_obj.get()["Body"].read().decode("utf-8")
    df = pd.read_csv(io.StringIO(file_content))
    
    return df

def process_and_insert_to_db():
    """Process CSV data from S3 and insert into PostgreSQL"""
    df = fetch_s3_file()

    if df is None or df.empty:
        print("No records found in the CSV file.")
        return

    # Convert 'mob' to string to avoid data type issues
    df["mob"] = df["mob"].astype(str)

    # Ensure no missing values in required fields
    df = df.dropna(subset=["customerId", "firstName", "lastName", "city", "mob"])

    # Prepare records for insertion
    records = df[['customerId', 'firstName', 'lastName', 'city', 'mob']].values.tolist()

    if not records:
        print("No valid records to insert into customers table.")
        return

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_test_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO customers (customerId, firstName, lastName, city, mob)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (customerId) DO UPDATE 
        SET firstName = EXCLUDED.firstName,
            lastName = EXCLUDED.lastName,
            city = EXCLUDED.city,
            mob = EXCLUDED.mob,
            updated_at = CURRENT_TIMESTAMP;
    """

    try:
        cursor.executemany(insert_sql, records)  # Bulk insert for better performance
        conn.commit()
        print(f"Successfully inserted {len(records)} records into customers table")
    except Exception as e:
        conn.rollback()  # Rollback if there's an error
        print(f"Database Insertion Error: {e}")
    finally:
        cursor.close()
        conn.close()

# Define Airflow tasks
fetch_s3_task = PythonOperator(
    task_id="fetch_s3_file",
    python_callable=fetch_s3_file,
    dag=dag
)

insert_db_task = PythonOperator(
    task_id="process_and_insert_to_db",
    python_callable=process_and_insert_to_db,
    dag=dag
)

# Set task dependencies
fetch_s3_task >> insert_db_task