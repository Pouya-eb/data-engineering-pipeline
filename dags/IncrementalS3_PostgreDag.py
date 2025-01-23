from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.hooks.base import BaseHook

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
    from pprint import pprint
    import pandas as pd
    import psycopg2
    import boto3

    # Set up connection details
    conn_id = 'Postgres_csv'  
    connection = BaseHook.get_connection(conn_id)
    host = connection.host
    dbname = "csv_database"
    user = connection.login
    password = connection.password
    port = connection.port

    # Task to print context for debugging
    @task
    def print_context(**context):
        pprint(context)

    # Task to process incremental CSV files
    @task
    def incremental_CSV():
        # Establish a connection to PostgreSQL
        conn = psycopg2.connect(host=host, user=user, password=password, port=port, dbname=dbname)
        cursor = conn.cursor()

        # Fetch IDs from the PostgreSQL table
        cursor.execute("SELECT _id FROM channels;")
        pg_ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
        conn.close()

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
                if 'channels' in file_name:
                    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
                    try:
                        df = pd.read_csv(obj['Body'])
                        all_data.append(df)
                    except pd.errors.ParserError as e:
                        error_files.append({'file': file_name, 'error': str(e)})
                
                # Break the loop if a JSON file is encountered
                elif '.json' in file_name:
                    break

            # If data was successfully read from S3
            if all_data:
                # Concatenate all data into a single DataFrame
                channels_df = pd.concat(all_data, ignore_index=True)

                # Extract IDs from the S3 data
                channels_ids = {id for id in channels_df['_id']}
                
                # Identify missing IDs by comparing with PostgreSQL data
                missing_ids = channels_ids - pg_ids
                missing_data = channels_df[channels_df['_id'].isin(missing_ids)]
                
                # Print some debug information
                print("First 10 rows of missing data:")
                print(missing_data.head(10))
                
                print("\nRow counts:")
                print(f"S3 table: {len(channels_df)} rows")
                print(f"Postgres table: {len(pg_ids)} rows")
                print(f"Difference: {len(missing_data)} rows")
            
            
            if error_files:
                print("\nFiles with errors:")
                for error in error_files:
                    print(f"File: {error['file']}, Error: {error['error']}")
        else:
            # Handle case where no files are found in the bucket
            print("No files found in the bucket.")

    
    print_context() >> incremental_CSV()


channel_S3()
