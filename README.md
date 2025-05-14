**1. Build docker image using docker file.**  
docker build -t airflow-v1:2.10.5 .

  
**2. Start the container using docker composer.**  
docker compose up


Access the airflow UI on http://localhost:8080

**Note:** Currently Airflow 2.10.5 is looking more stable, don't try to upgrade the airflow

**Customisations in airflow.cfg file[update the key values]:**   

**3. Enabling test connection & changing airflow db from default sql** 
test_connection = Enabled [Enables test connection button in airflow UI.]  
sql_alchemy_conn = postgresql+psycopg2://airflow_user:airflow123@host.docker.internal/airflow_db_v2 [If you want to take from env variable change the line like sql_alchemy_conn = ${DB_URL} then pass this DB_URL from docker-compose file vai env]

**4. Enabling remote logging:**  
remote_logging = True  
remote_log_conn_id = aws_s3 [create connection from UI with conn id as aws_s3]  
remote_base_log_folder = s3://313142695293-logs/airflow-logs/ [s3://bucket-name/folder-name/]

**5. smtp email configuration**  
smtp_host = smtp.gmail.com  
smtp_user = vinayakvinayak627@gmail.com  
smtp_password = kkatjothfhhyrynr [Google app password, created form manage google account]  
smtp_port = 587  
smtp_mail_from = vinayakvinayak627@gmail.com

**Important Note:**  
When you are running in dev, according to Dockerfile all dags present in dags folder will be deleted except dynamic_dag_generator.py and airflow will import from s3 bucket.

**Environment Variables:**  
AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "True"
AIRFLOW_DATABASE: postgresql+psycopg2://airflow_user:airflow123@host.docker.internal/airflow_db_v2
AIRFLOW_LOGGING_REMOTE_BASE_LOG_FOLDER: s3://313142695293-logs/airflow-logs/
AIRFLOW_LOGGING_REMOTE_LOG_CONN_ID: zaggle_s3
AIRFLOW_REMOTE_DAGS_FOLDER: s3://313142695293-logs/airflow-logs/
AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}  # Or your preferred region   

**Docker run command**  
docker run -d \
  --name airflow-container \
  -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID=AKIAUR2GCKV63UXI42Q4 \
  -e AWS_SECRET_ACCESS_KEY=yaA/nkpMOepc8YxfxLxqm1uI8Ug1tndjwWzdjo/v \
  -e AWS_DEFAULT_REGION=ap-south-1 \
  -e AIRFLOW__WEBSERVER__EXPOSE_CONFIG="True"\
  -e AIRFLOW_DATABASE=postgresql+psycopg2://airflow_user:airflow123@host.docker.internal/airflow_db_v2\
  -e AIRFLOW_LOGGING_REMOTE_BASE_LOG_FOLDER=s3://313142695293-logs/dags/\
  -e AIRFLOW_REMOTE_DAGS_FOLDER="s3://313142695293-logs/dags/" \
  -e ZIG_MONGO_DB_URI=mongodb://v2:v2@host.docker.internal:27017 \
  -e ZIG_MONGO_DB=zig-core-svc \
  airflowservice:2.10.5
