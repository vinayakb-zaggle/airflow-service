from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import requests
from datetime import datetime, timedelta

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print('Today is {}'.format(datetime.today().date()))

def fetch_and_store_posts():
    """
    Fetches data from the API and stores it in the PostgreSQL database.
    """
    try:
        url = 'https://jsonplaceholder.typicode.com/posts'
        response = requests.get(url)
        response.raise_for_status()  # Raises an exception for HTTP errors
        
        jsonResponse = response.json()
        
        # Get PostgreSQL connection from Airflow
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_test_db')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Insert data into posts table
        insert_query = """
        INSERT INTO posts (user_id, id, title, body) VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """

        for post in jsonResponse:
            cursor.execute(insert_query, (post["userId"], post["id"], post["title"], post["body"]))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print('Successfully inserted API data into PostgreSQL.')

    except requests.exceptions.RequestException as e:
        print("Error fetching response:", e)
    except Exception as e:
        print("An unexpected error occurred:", e)

# Define the DAG
dag = DAG(
    'api_to_postgres',
    default_args={
        'start_date': days_ago(1),
        'owner': 'Vinayak',
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
    },
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['api', 'postgres']
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

fetch_and_store_posts_task = PythonOperator(
    task_id='fetch_and_store_posts',
    python_callable=fetch_and_store_posts,
    dag=dag
)

# Set the task dependencies
print_welcome_task >> print_date_task >> fetch_and_store_posts_task