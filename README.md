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
