from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.hooks.base import BaseHook
import pandas as pd
import psycopg2
import boto3

# Default arguments for the DAG
default_args = {
    "owner": "US",
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

    # Set up connection details
    conn_id = 'Postgres_csv'  
    connection = BaseHook.get_connection(conn_id)
    host = connection.host
    dbname = "csv_database"
    user = connection.login
    password = connection.password
    port = connection.port

    @task
    def fetch_missing_data():
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
            aws_access_key_id='ab5fc903-7426-4a49-ae3e-024b53c30d27',
            aws_secret_access_key='f70c316b936ffc50668d21442961339a90b627daa190cff89e6a395b821001f2',
            endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir'
        )

        # Define the S3 bucket name
        bucket_name = 'qbc'

        # Fetch the list of objects from the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)

        # Initialize data containers
        all_data = []
        error_files = []

        # Process files if found
        if 'Contents' in response:
            for file in response['Contents']:
                file_name = file['Key']

                # Process CSV files containing 'channels' in the name
                if 'channels' in file_name and '.csv' in file_name:
                    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                    try:
                        df = pd.read_csv(obj['Body'])
                        all_data.append(df)
                    except pd.errors.ParserError as e:
                        error_files.append({'file': file_name, 'error': str(e)})

            # If data was successfully read from S3
            if all_data:
                # Concatenate all data into a single DataFrame
                channels_df = pd.concat(all_data, ignore_index=True)

                # Extract IDs from the S3 data
                channels_ids = {id for id in channels_df['_id']}

                # Identify missing IDs by comparing with PostgreSQL data
                missing_ids = channels_ids - pg_ids
                missing_data = channels_df[channels_df['_id'].isin(missing_ids)]

                # Print debug information
                print("First 10 rows of missing data:")
                print(missing_data.head(10))

                print("\nRow counts:")
                print(f"S3 table: {len(channels_df)} rows")
                print(f"Postgres table: {len(pg_ids)} rows")
                print(f"Difference: {len(missing_data)} rows")

                return missing_data.to_dict('records')  # Return missing data as a list of dictionaries

        return []

    @task
    def insert_missing_data(missing_data):
        if not missing_data:
            print("No missing data to insert.")
            return

        # Establish a connection to PostgreSQL
        conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
        cursor = conn.cursor()

        dest_columns = [
            'created_at', '_id', 'username', 'userid', 'avatar_thumbnail', 'is_official', 'name',
            'bio_links', 'total_video_visit', 'video_count', 'start_date', 'start_date_timestamp',
            'followers_count', 'following_count', 'country', 'platform', 'update_count'
        ]

        # Insert rows into PostgreSQL
        for row in missing_data:
            values = [row.get(col, None) for col in dest_columns]

            # Insert query
            insert_query = """
            INSERT INTO channels (created_at, _id, username, userid, avatar_thumbnail, is_official, name,
                                bio_links, total_video_visit, video_count, start_date, start_date_timestamp,
                                followers_count, following_count, country, platform, update_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            cursor.execute(insert_query, values)

        conn.commit()
        cursor.close()
        conn.close()

    # Define the DAG structure
    missing_data = fetch_missing_data()
    insert_missing_data(missing_data)

channel_S3()