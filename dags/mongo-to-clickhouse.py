from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


def get_conn_mongo():
    hook = MongoHook(mongo_conn_id="my_mongo_database")
    return hook.get_conn()


def get_conn_clickhouse():
    return ClickHouseHook(clickhouse_conn_id="my_clickhouse_database")


def extracting_object(row):
    obj = row.get("object", {})
    return (
        row.get("_id"),
        row.get("created_at"),
        row.get("expire_at"),
        row.get("is_produce_to_kafka"),
        obj.get("platform"),
        obj.get("id"),
        obj.get("owner_username"),
        obj.get("owner_id"),
        obj.get("title"),
        obj.get("tags"),
        obj.get("uid"),
        obj.get("visit_count"),
        obj.get("owner_name"),
        obj.get("poster"),
        obj.get("owner_avatar"),
        obj.get("duration"),
        datetime.strptime(obj.get("posted_date"), "%Y-%m-%d %H:%M:%S"),
        obj.get("posted_timestamp"),
        datetime.strptime(obj.get("sdate_rss"), "%Y-%m-%d %H:%M:%S"),
        obj.get("sdate_rss_tp"),
        obj.get("comments"),
        obj.get("frame"),
        obj.get("like_count"),
        obj.get("description"),
        obj.get("is_deleted"),
        row.get("update_count"),
    )


def creating_table_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("CREATE DATABASE IF NOT EXISTS bronze;")
    hook.execute("DROP TABLE IF EXISTS bronze.videos;")
    hook.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.videos (
            
            _id                     String,
            created_at              DATETIME,
            expire_at               DATETIME,
            is_produce_to_kafka     BOOLEAN,
            platform                Nullable(String),
            id                      Nullable(INTEGER),
            owner_username          Nullable(String),
            owner_id                Nullable(String),
            title                   Nullable(String),
            tags                    Nullable(String),
            uid                     Nullable(String),
            visit_count             Nullable(INTEGER),
            owner_name              Nullable(String),
            poster                  Nullable(String),
            owner_avatar            Nullable(String),
            duration                Nullable(INTEGER),
            posted_date             Nullable(DATETIME),
            posted_timestamp        Nullable(INTEGER),
            sdate_rss               Nullable(DATETIME),
            sdate_rss_tp            Nullable(INTEGER),
            comments                Nullable(String),
            frame                   Nullable(String),
            like_count              Nullable(INTEGER),
            description             Nullable(String),
            is_deleted              Nullable(BOOLEAN),
            update_count            INTEGER
            
        ) ENGINE = MergeTree()
          ORDER BY _id;
        """
    )


def importing_data_to_clickhouse():

    client = get_conn_mongo()
    database = client["mydb"]
    collection = database["videos"]

    hook = get_conn_clickhouse()

    last_id = None
    batch_size = 1000

    while True:
        query = {} if last_id is None else {"_id": {"$gt": last_id}}
        batch = list(collection.find(query).sort({"_id": 1}).limit(batch_size))

        if not batch:
            break

        last_id = batch[-1]["_id"]
        rows = [extracting_object(row) for row in batch]

        hook.execute("INSERT INTO bronze.videos VALUES", rows)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="mongo-to-clickhouse",
    default_args=default_args,
    description="importing data from mongo into clickhouse (bronze_layer)",
    schedule="@once",
    catchup=False,
    tags=["mongo", "clickhouse"],
) as dag:

    creating_table_clickhouse_task = PythonOperator(
        task_id="creating_table_clickhouse", python_callable=creating_table_clickhouse
    )

    importing_data_to_clickhouse_task = PythonOperator(
        task_id="importing_data_from_mongo_to_clickhouse_task",
        python_callable=importing_data_to_clickhouse,
    )


creating_table_clickhouse_task >> importing_data_to_clickhouse_task