from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_conn():
    hook = PostgresHook(postgres_conn_id="my_postgres_database")
    return hook.get_conn()


def creating_table():
    connection = get_conn()
    cursor = connection.cursor()

    cursor.execute(
        """
        DROP TABLE IF EXISTS channels;
        
        CREATE TABLE IF NOT EXISTS channels (
            _id                     TEXT,
            username                TEXT,
            userid                  TEXT,
            avatar_thumbnail        TEXT,
            is_official             BOOLEAN,
            name                    TEXT,
            bio_links               TEXT,
            total_video_visit       BIGINT,
            video_count             INTEGER,
            start_date              TIMESTAMP,
            start_date_timestamp    BIGINT,
            followers_count         INTEGER,
            following_count         INTEGER,
            country                 TEXT,
            platform                TEXT,
            created_at              TIMESTAMP,
            update_count            INTEGER
        );
        """
    )
    connection.commit()
    cursor.close()


def inserting_data():
    DATA_PATH = Variable.get("MY_POSTGRES_DATA_PATH")
    connection = get_conn()
    cursor = connection.cursor()

    cursor.execute(
        f"""
        COPY channels
        FROM '{DATA_PATH}'
        DELIMITER ','
        CSV HEADER;
        """
    )

    connection.commit()
    cursor.close()
    connection.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres-init",
    default_args=default_args,
    description="Importing data to postgres",
    schedule="@once",
    catchup=False,
    tags=["postgres"],
) as dag:

    creating_table_task = PythonOperator(
        task_id="postgres_creating_table_task",
        python_callable=creating_table,
    )

    importing_data_task = PythonOperator(
        task_id="postgres_importing_data_task",
        python_callable=inserting_data,
    )

creating_table_task >> importing_data_task
