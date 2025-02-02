from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pandas as pd
import psycopg2
import boto3
from telegram_bot import Bot


def telegram_callback(context):
    dag = context.get("dag")
    Bot().send_message(dag)

# Default arguments for the DAG
default_args = {
    "owner": "US",
    "on_failure_callback": telegram_callback,
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG using the @dag decorator
@dag(
    dag_id='channelS3Incremental',
    description='Incremental Load on Postgre',
    schedule_interval='0 0 * * *',  
    default_args=default_args,
    catchup=False,
    tags=['Incremental', 'S3-channels']
)
def channel_S3():

    @task
    def fetch_and_insert_missing_data():
        # Set up connection details
        conn_id = 'Postgres_csv'
        connection = BaseHook.get_connection(conn_id)
        host = connection.host
        dbname = connection.schema
        user = connection.login
        password = connection.password
        port = connection.port

        # Establish a connection to PostgreSQL
        conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
        cursor = conn.cursor()

        # Fetch IDs from the PostgreSQL table
        cursor.execute("SELECT _id FROM channels;")
        pg_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()

        # Configure S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=Variable.get("aws_access_key_id"),
            aws_secret_access_key=Variable.get("aws_secret_access_key"),
            endpoint_url=Variable.get("endpoint_url")
        )

        # Define the S3 bucket name
        bucket_name = Variable.get("bucket_name")

        # Fetch the list of objects from the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Initialize data containers
        all_data = []
        error_files = []
        dest_columns = [
            'created_at', '_id', 'username', 'userid', 'avatar_thumbnail', 'is_official', 'name',
            'bio_links', 'total_video_visit', 'video_count', 'start_date', 'start_date_timestamp',
            'followers_count', 'following_count', 'country', 'platform', 'update_count'
        ]
        insert_query = """
            INSERT INTO channels (created_at, _id, username, userid, avatar_thumbnail, is_official, name,
            bio_links, total_video_visit, video_count, start_date, start_date_timestamp,
            followers_count, following_count, country, platform, update_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        # Process files if found
        if 'Contents' in response:
            for file in response['Contents']:
                file_name = file['Key']

                # Process CSV files containing 'channels' in the name
                if 'channels' in file_name and '.csv' in file_name:
                    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                    try:
                        df = pd.read_csv(obj['Body'])
                        channels_ids = {id for id in df['_id']}
                        missing_ids = channels_ids - pg_ids
                        missing_data = df[df['_id'].isin(missing_ids)]
                        print(f"S3 table: {len(df)} rows")
                        print(f"Postgres table: {len(pg_ids)} rows")
                        print(f"Difference: {len(missing_data)} rows")
                        if not missing_data.empty:
                            cursor = conn.cursor()
                            for row in missing_data.to_dict('records'):
                                values = [row.get(col, None) for col in dest_columns]
                                cursor.execute(insert_query, values)
                    except pd.errors.ParserError as e:
                        error_files.append({'file': file_name, 'error': str(e)})
            
            conn.commit()
            print("Step 2 Done !!")
        cursor.close()
        conn.close()
    # Define the DAG structure
    fetch_and_insert_missing_data()

channel_S3()
