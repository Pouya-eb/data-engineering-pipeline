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
            country                 String,
            channel_counts          AggregateFunction(count, String),
            follower_counts         AggregateFunction(avg, UInt64),
            video_visits            AggregateFunction(avg, UInt64)
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
            countState(channel_userid)                           AS channel_counts,
            avgState(CAST(channel_followers_count AS UInt64))    AS follower_counts,
            avgState(CAST(video_visit_count AS UInt64))          AS video_visits
        FROM silver.Videos_channels_OBT
        WHERE channel_country IS NOT NULL
            AND channel_coountry != ''
        GROUP BY channel_country;
        """
    )


def initializeing_table():
    hook = get_conn_clickhouse()
    hook.execute(
        """
        INSERT INTO geo_distribution 
            (country, channel_counts, follower_counts, video_visits)
        SELECT
            channel_country,
            countState(channel_userid),
            avgState(CAST(channel_followers_count AS UInt64)),
            avgState(CAST(video_visit_count AS UInt64))
        FROM silver.Videos_channels_OBT
        WHERE channel_country IS NOT NULL
            AND channel_coountry != ''
        GROUP BY channel_country;        
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
