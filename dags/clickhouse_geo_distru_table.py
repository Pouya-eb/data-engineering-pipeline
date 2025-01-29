from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def get_conn_clickhouse():
    return ClickHouseHook(clickhouse_conn_id="my_clickhouse_database")


def creating_table_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("CREATE DATABASE IF NOT EXISTS gold;")
    hook.execute("DROP TABLE IF EXISTS gold.geo_distribution;")
    hook.execute(
        """
        CREATE TABLE IF NOT EXISTS gold.geo_distribution (
            country                         String,
            total_channel_counts            AggregateFunction(count, String),
            largest_follower_counts         AggregateFunction(max, UInt64),
            average_follower_counts         AggregateFunction(avg, UInt64),
            average_video_visits            AggregateFunction(avg, UInt64)
        ) ENGINE = AggregatingMergeTree()
          ORDER BY country;
        """
    )


def creating_materialized_view_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("DROP TABLE IF EXISTS gold.geo_distribution_mv;")
    hook.execute(
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS gold.geo_distribution_mv
        TO gold.geo_distribution AS
        SELECT 
            channel_country                                      AS country,
            countState(channel_userid)                           AS total_channel_counts,
            maxState(CAST(channel_follower_counts AS UInt64))    As largest_follower_counts,
            avgState(CAST(channel_followers_count AS UInt64))    AS average_follower_counts,
            avgState(CAST(video_visit_count AS UInt64))          AS average_video_visits
        FROM silver.Videos_channels_OBT
        WHERE channel_country IS NOT NULL
            AND channel_country != ''
        GROUP BY channel_country;
        """
    )


def initializeing_table():
    hook = get_conn_clickhouse()
    hook.execute(
        """
        INSERT INTO gold.geo_distribution 
            (country, total_channel_counts, largest_follower_counts, average_follower_counts, average_video_visits)
        SELECT
            channel_country,
            countState(channel_userid),
            maxState(CAST(channel_followers_count AS UInt64)),
            avgState(CAST(channel_followers_count AS UInt64)),
            avgState(CAST(video_visit_count AS UInt64))
        FROM silver.Videos_channels_OBT
        WHERE channel_country IS NOT NULL
            AND channel_country != ''
        GROUP BY channel_country;        
        """
    )


default_args = {
    "owner": "pouya",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="geo_distro_table",
    default_args=default_args,
    description="creating table geo_distru to report performance of each channel base on its location",
    schedule="@once",
    catchup=False,
    tags=["gold", "clickhouse"],
) as dag:

    creating_table_clickhouse_task = PythonOperator(
        task_id="creating_table_geo_distribution",
        python_callable=creating_table_clickhouse,
    )

    creating_materialized_view_clickhouse_task = PythonOperator(
        task_id="creating_materialized_view",
        python_callable=creating_materialized_view_clickhouse,
    )

    initializeing_table_task = PythonOperator(
        task_id="initializing_table_geo_distribution",
        python_callable=initializeing_table,
    )


creating_table_clickhouse_task >> creating_materialized_view_clickhouse_task
creating_materialized_view_clickhouse_task >> initializeing_table_task
