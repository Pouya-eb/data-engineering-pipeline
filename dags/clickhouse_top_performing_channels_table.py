from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id='top_performing_channels',
    description='a dag to create top performing channels report',
    schedule='@once',
    default_args=default_args,
    catchup=False,
    tags=['clickhouse', 'channel-Rep']        
)
def top_perform_ch():
    from pprint import pprint
    from clickhouse_driver import Client
    clickhouse_engine = ClickHouseHook(clickhouse_conn_id='clickhouse_tcp')    


    @task
    def print_context(**context):
        pprint(context)

    @task
    def top_performing_channels():
        client = Client('82.115.20.70', port=9000)
        # Queries
        create_db_query = "CREATE DATABASE IF NOT EXISTS gold"

        drop_table_query = """
        DROP TABLE IF EXISTS gold.top_performing_channels
        """

        create_table_query = """
        CREATE TABLE gold.top_performing_channels
        (
            channel_userid String,
            total_followers_count UInt64,
            total_video_visits UInt64,
            total_video_count UInt64
        )
        ENGINE = AggregatingMergeTree
        ORDER BY (channel_userid)
        SETTINGS index_granularity = 8192;
        """

        insert_data_query = """
        INSERT INTO gold.top_performing_channels
        SELECT
            channel_userid,
            sum(channel_followers_count) AS total_followers_count,
            sum(channel_total_video_visit) AS total_video_visits,
            count(video_id) AS total_video_count
        FROM silver.Videos_channels_OBT
        GROUP BY channel_userid;
        """



        client.execute(create_db_query)
        print("Database 'gold' ensured to exist.")
            
        client.execute(drop_table_query)
        print("Dropped table 'top_performing_channels' if it existed.")
            
        client.execute(create_table_query)
        print("Created table 'top_performing_channels'.")
            
        client.execute(insert_data_query)
        print("Inserted aggregated data into 'top_performing_channels'.")
            
            
    print_context() >> top_performing_channels()


top_perform_ch()