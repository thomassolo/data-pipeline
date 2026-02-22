import os 
import boto3
import snowflake.connector
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_BUCKET = os.getenv("MINIO_BUCKET")

s3_client = boto3.client('s3',
                  endpoint_url = "http://minio:9000",
                  aws_access_key_id = "admin",
                  aws_secret_access_key = "password123")

SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_TABLE = os.getenv("SF_TABLE")

def load_data_tosnowflake():
    response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET)
    if 'Contents' not in response:
        print("No objects found in the bucket.")
        return
    #récup des fichiers à charger, dans une liste
    files_to_load = [obj['Key'] for obj in response['Contents']]
    if not files_to_load:
        print("No files to load.")
        return
    conn = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )
    cursor = conn.cursor()
    for file_name in files_to_load:
        print(f"Loading file: {file_name}")
        local_path = f"/tmp/{file_name}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        try:
            s3_client.download_file(MINIO_BUCKET, file_name, local_path)
        except Exception as e:
            print(f"Error downloading file {file_name}: {e}")
            continue
        query = f"PUT file://{local_path} @%{SF_TABLE} AUTO_COMPRESS=TRUE"
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"Error executing PUT query for file {file_name}: {e}")
            continue
        #basename du fichier pour la requete COPY INTO
        file_basename = os.path.basename(file_name)
        #copie du fichier dl dans la table snowflake 
        cp_query = f"""COPY INTO {SF_TABLE}
                        FROM @%{SF_TABLE}
                        FILES = ('{file_basename}.gz')
                        FILE_FORMAT = (TYPE = 'JSON')
                        ON_ERROR = 'CONTINUE';"""
        try:
            cursor.execute(cp_query)
        except Exception as e:
            print(f"Error executing COPY query for file {file_name}: {e}")
        os.remove(local_path)
    cursor.close()
    conn.close()

default_args = {
    'owner': 'solo',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'minio_to_snowflake',
    default_args=default_args,
    description='A DAG to load data from MinIO to Snowflake',
    schedule_interval="@hourly",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    tags = ('ingestion', 'snowflake', 'minio')
) as dag:
    load_bronze_task = PythonOperator(
        task_id = 'load_data_to_snowflake',
        python_callable = load_data_tosnowflake
    )