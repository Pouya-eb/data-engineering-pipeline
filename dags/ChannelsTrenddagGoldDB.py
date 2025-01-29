from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from telegram_bot import Bot


def telegram_callback(context):
    dag = context.get("dag")
    Bot().send_message(dag)


default_args = {
    "owner": "Mkz",
    "on_failure_callback": telegram_callback,
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id='channelviewRep',
    description='a dag to create channel_growth_report',
    schedule='@once',
    default_args=default_args,
    catchup=False,
    tags=['clickhouse', 'channel-Rep']        
)
def videos_bt():
    from pprint import pprint
    from clickhouse_driver import Client
    clickhouse_engine = ClickHouseHook(clickhouse_conn_id='clickhouse_tcp')    


    @task
    def print_context(**context):
        pprint(context)

    @task
    def create_channel_Growth():
        client = Client('82.115.20.70', port=9000)
        # Queries
        create_db_query = "CREATE DATABASE IF NOT EXISTS gold"

        drop_table_query = """
        DROP TABLE IF EXISTS gold.channels_view_growth
        """

        create_table_query = """
        CREATE TABLE gold.channels_view_growth
        (
            channel_userid String,
            date Date,
            total_followers_count UInt64,
            total_video_visits UInt64,
            total_video_count UInt64,
            agg_state_followers AggregateFunction(sum, UInt64),
            agg_state_video_visits AggregateFunction(sum, UInt64),
            agg_state_video_count AggregateFunction(sum, UInt64)
        )
        ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (channel_userid, date)
        SETTINGS index_granularity = 8192;
        """

        insert_data_query = """
        INSERT INTO gold.channels_view_growth
        SELECT
            channel_userid,
            toDate(video_posted_date) AS date,
            sum(channel_followers_count) AS total_followers_count,
            sum(channel_total_video_visit) AS total_video_visits,
            count(video_id) AS total_video_count,
            sumState(toUInt64(channel_followers_count)) AS agg_state_followers,
            sumState(toUInt64(channel_total_video_visit)) AS agg_state_video_visits,
            sumState(toUInt64(1)) AS agg_state_video_count
        FROM silver.Videos_channels_OBT
        GROUP BY channel_userid, date
        """



        client.execute(create_db_query)
        print("Database 'gold' ensured to exist.")
            
        client.execute(drop_table_query)
        print("Dropped table 'channels_view_growth' if it existed.")
            
        client.execute(create_table_query)
        print("Created table 'channels_view_growth'.")
            
        client.execute(insert_data_query)
        print("Inserted aggregated data into 'channels_view_growth'.")
            
            
    print_context() >> create_channel_Growth()


videos_bt()