from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def get_clickhouse_hook():
    return ClickHouseHook(clickhouse_conn_id="my_clickhouse_database")


def create_clickhouse_table():
    hook = get_clickhouse_hook()
    hook.execute("CREATE DATABASE IF NOT EXISTS bronze;")
    hook.execute("""
        CREATE TABLE IF NOT EXISTS bronze.channels
        (
            _id                  String,
            username             String,
            userid               String,
            avatar_thumbnail     String,
            is_official          Bool,
            name                 String,
            bio_links            String,
            total_video_visit    Int64,
            video_count          Int32,
            start_date           DateTime,
            start_date_timestamp Int64,
            followers_count      Int32,
            following_count      Int32,
            country              String,
            platform             String,
            created_at           DateTime,
            update_count         Int32
        ) ENGINE = MergeTree
        order by _id;
    """)


def load_data_from_postgres():
    hook = get_clickhouse_hook()
    hook.execute(f"""
        INSERT INTO bronze.channels
        SELECT *
        FROM postgresql('{Variable.get('postgres_csv_host')}', '{Variable.get("postgres_csv_db")}', 'channels', '{Variable.get("postgres_csv_user")}', '{Variable.get("postgres_csv_password")}');
    """)


default_args = {
    'owner': 'Kavian',
    'depends_on_past': False,
    'email': ['kavianam@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

with DAG(
    'load_postgres_channels_to_clickhouse',
    'Load channels data in Postgres into ClickHouse',
    '@once',
    start_date=pendulum.datetime(2025, 1, 11, tz="Asia/Tehran"),
    tags=['clickhouse', 'postgres', 'bronze'],
):
    START = EmptyOperator(task_id='START')
    END = EmptyOperator(task_id='END')

    create_table = PythonOperator(
        task_id='create_clickhouse_channels_table',
        python_callable=create_clickhouse_table,
    )

    load_channels_data = PythonOperator(
        task_id='load_postgres_channels_to_clickhouse',
        python_callable=load_data_from_postgres,
    )

    START >> create_table >> load_channels_data >> END
