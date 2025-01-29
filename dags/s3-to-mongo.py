import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient, errors
import boto3
from airflow.hooks.base import BaseHook
from telegram_bot import Bot
from airflow.decorators import dag
###
def telegram_callback(context):
    dag = context.get("dag")
    Bot().send_message(dag)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "maj-vh",
    "on_failure_callback": telegram_callback,
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def create_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT_URL'),
        aws_access_key_id=os.getenv('S3_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY')
    )

def create_mongo_client():
    connection_uri = BaseHook.get_connection("my_mongo_database").get_uri()
    
    if connection_uri.startswith("mongo://"):
        connection_uri = connection_uri.replace("mongo://", "mongodb://", 1)

    return MongoClient(connection_uri)

@dag(
    dag_id='s3_to_mongodb_pipeline',
    description='Incremental Load from S3 to MongoDB',
    schedule_interval='0 0 * * *',
    default_args=default_args,
    catchup=False,
    tags=['Incremental', 'S3', 'MongoDB']
)
def s3_to_mongodb_dag():
    def list_s3_files(ti, execution_date):
        s3 = create_s3_client()
        bucket_name = os.getenv('S3_BUCKET_NAME')

        if isinstance(execution_date, str):
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d")

        target_date = execution_date.strftime("%Y-%m-%d")

        response = s3.list_objects_v2(Bucket=bucket_name)

        json_files = [
            file['Key'] for file in response.get('Contents', [])
            if file['Key'].endswith(".json") and target_date in file['Key']
        ]

        logger.info(f"Found JSON files for {target_date}: {json_files}")
        ti.xcom_push(key='json_files', value=json_files)

    task_list_s3_files = PythonOperator(
        task_id='list_s3_files',
        python_callable=list_s3_files,
        op_kwargs={'execution_date': '{{ ds }}'},
    )

    def download_s3_files(ti):
        json_files = ti.xcom_pull(task_ids='list_s3_files', key='json_files')

        s3 = create_s3_client()
        mongo_client = create_mongo_client()
        db = mongo_client["mydb"]
        batch_collection = db["batch"]

        path_write_json = os.getenv('PATH_WRITE_JSON')
        os.makedirs(path_write_json, exist_ok=True)

        for file in json_files:
            file_id = os.path.basename(file)

            if batch_collection.find_one({"file_name": file_id}):
                logger.info(f"File already processed: {file_id}")
                continue

            local_file_path = os.path.join(path_write_json, file_id)
            if not os.path.exists(local_file_path):
                logger.info(f"Downloading file: {file}")
                s3.download_file(os.getenv('S3_BUCKET_NAME'), file, local_file_path)

    task_download_s3_files = PythonOperator(
        task_id='download_s3_files',
        python_callable=download_s3_files,
    )

    def insert_to_mongodb():
        mongo_client = create_mongo_client()
        db = mongo_client["mydb"]
        videos_collection = db["videos"]
        batch_collection = db["batch"]

        path_write_json = os.getenv('PATH_WRITE_JSON')

        logger.info(f"Checking for JSON files in: {path_write_json}")

        for file_name in os.listdir(path_write_json):
            if not file_name.endswith(".json"):
                logger.info(f"Skipping non-JSON file: {file_name}")
                continue

            file_path = os.path.join(path_write_json, file_name)

            if batch_collection.find_one({"file_name": file_name}):
                logger.info(f"Skipping already processed file: {file_name}")
                continue

            try:
                with open(file_path, 'r') as file:
                    records = []
                    for line in file:
                        try:
                            record = json.loads(line.strip())  
                            records.append(record)
                        except json.JSONDecodeError as e:
                            logger.warning(f"JSONDecodeError: {e} in file {file_name}, line: {line}")
                            continue

                    if records:
                        try:
                            videos_collection.insert_many(records, ordered=False)
                            logger.info(f"Inserted {len(records)} records into 'videos'.")
                        except errors.BulkWriteError as bwe:
                            inserted_count = len([err for err in bwe.details["writeErrors"] if err["code"] != 11000])
                            logger.warning(f"BulkWriteError: {bwe.details}. Inserted {inserted_count} valid records.")

                batch_collection.insert_one({"file_name": file_name})
                logger.info(f"Processed and marked file as completed: {file_name}")

            except Exception as e:
                logger.error(f"Error processing file {file_name}: {str(e)}", exc_info=True)

    task_insert_to_mongodb = PythonOperator(
        task_id='insert_to_mongodb',
        python_callable=insert_to_mongodb,
    )

    task_list_s3_files >> task_download_s3_files >> task_insert_to_mongodb

dag = s3_to_mongodb_dag()
