from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime, duration


def get_clickhouse_hook():
    return ClickHouseHook(clickhouse_conn_id="my_clickhouse_database")


def get_postgres_hook():
    return PostgresHook(postgres_conn_id="my_postgres_database")


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


def incremental_load_data_from_postgres():
    hook = get_clickhouse_hook()
    hook.execute(f"""
        INSERT INTO bronze.channels
        SELECT *
        FROM postgresql('{Variable.get('postgres_csv_host')}', '{Variable.get("postgres_csv_db")}', 'channels', '{Variable.get("postgres_csv_user")}', '{Variable.get("postgres_csv_password")}')
        WHERE start_date > (SELECT toString(MAX(start_date)) FROM bronze.channels);
    """)


def data_validity_check():
    postgres_hook = get_postgres_hook()
    ch_hook = get_clickhouse_hook()

    # Row Count
    postgres_row_count = postgres_hook.get_records("""
        SELECT COUNT(*)
        FROM channels
    """)[0][0]
    clickhouse_row_count = ch_hook.execute("""
        SELECT COUNT(*)
        FROM bronze.channels;
    """)[0][0]

    print(f'Postgres row count: {postgres_row_count}')
    print(f'ClickHouse row count: {clickhouse_row_count}')

    if postgres_row_count != clickhouse_row_count:
        raise ValueError(f"Postgres row count: {postgres_row_count} - ClickHouse row count: {clickhouse_row_count}")

    # Column Count
    postgres_column_count = postgres_hook.get_records("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = 'channels';
    """)[0][0]
    clickhouse_column_count = ch_hook.execute("""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'channels';
    """)[0][0]

    print(f'Postgres column count: {postgres_column_count}')
    print(f'ClickHouse column count: {clickhouse_column_count}')

    if postgres_column_count != clickhouse_column_count:
        raise ValueError(f"Postgres column count: {postgres_column_count} - ClickHouse column count: {clickhouse_column_count}")


default_args = {
    'owner': 'Kavian',
    'depends_on_past': False,
    'email': ['kavianam@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': duration(minutes=1),
    'catchup': False,
}

with DAG(
    'load_postgres_channels_to_clickhouse',
    'Load channels data in Postgres into ClickHouse',
    '@daily',
    start_date=datetime(2025, 1, 11, tz="Asia/Tehran"),
    tags=['clickhouse', 'postgres', 'bronze'],
):
    START = EmptyOperator(task_id='START')
    END = EmptyOperator(task_id='END')

    create_table = PythonOperator(
        task_id='create_clickhouse_channels_table',
        python_callable=create_clickhouse_table,
        retries=3,
        retry_delay=duration(minutes=1),
    )

    load_channels_data = PythonOperator(
        task_id='incremental_load_postgres_channels_to_clickhouse',
        python_callable=incremental_load_data_from_postgres,
        retries=3,
        retry_delay=duration(minutes=1),
    )

    check_data_validity = PythonOperator(
        task_id='data_validity_check',
        python_callable=data_validity_check,
        retries=3,
        retry_delay=duration(minutes=1),
    )

    START >> create_table >> load_channels_data >> check_data_validity >> END
