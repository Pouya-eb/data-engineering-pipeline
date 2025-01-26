from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def get_conn_clickhouse():
    return ClickHouseHook(clickhouse_conn_id="my_clickhouse_database")


def create_table_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("CREATE DATABASE IF NOT EXISTS gold;")
    hook.execute("DROP TABLE IF EXISTS gold.top_performing_channels;")
    hook.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.top_performing_channels
        (
            channel_userid String,
            total_followers_count UInt64,
            total_video_visits UInt64,
            total_video_count UInt64
        )
        ENGINE = AggregatingMergeTree
        ORDER BY (channel_userid);
        """
    )


def create_materialized_view_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("DROP TABLE IF EXISTS gold.top_performing_channels_mv;")
    hook.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS gold.top_performing_channels_mv
        TO gold.top_performing_channels AS
        SELECT
            channel_userid,
            SUM(channel_followers_count) AS total_followers_count,
            SUM(channel_total_video_visit) AS total_video_visits,
            COUNT(video_id) AS total_video_count
        FROM silver.Videos_channels_OBT
        GROUP BY channel_userid;
        """
    )


def initialize_table():
    hook = get_conn_clickhouse()
    hook.execute(
        """
        INSERT INTO gold.top_performing_channels
        SELECT
            channel_userid,
            sum(channel_followers_count) AS total_followers_count,
            sum(channel_total_video_visit) AS total_video_visits,
            count(video_id) AS total_video_count
        FROM silver.Videos_channels_OBT
        GROUP BY channel_userid;
        """
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id='top_performing_channels',
    description='a dag to create top performing channels report',
    schedule='@once',
    default_args=default_args,
    catchup=False,
    tags=['gold', 'clickhouse']        
) as dag:

    create_table_clickhouse_task = PythonOperator(
        task_id="create_table_top_performing_ch",
        python_callable=create_table_clickhouse,
    )

    create_mv_clickhouse_task = PythonOperator(
        task_id="create_mv",
        python_callable=create_materialized_view_clickhouse,
    )

    initialize_table_task = PythonOperator(
        task_id="initialize_table_top_performing_ch",
        python_callable=initialize_table,
    )


create_table_clickhouse_task >> create_mv_clickhouse_task
create_mv_clickhouse_task >> initialize_table_task