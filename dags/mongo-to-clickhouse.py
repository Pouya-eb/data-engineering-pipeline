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
    obj = row["object"]
    return (
        row["_id"],
        row["created_at"],
        row["expire_at"],
        row["is_produce_to_kafka"],
        obj["platform"],
        obj["id"],
        obj["owner_username"],
        obj["owner_id"],
        obj["title"],
        obj["tags"],
        obj["uid"],
        obj["visit_count"],
        obj["owner_name"],
        obj["poster"],
        obj["owner_avatar"],
        obj["duration"],
        obj["posted_date"],
        obj["posted_timestamp"],
        obj["sdate_rss"],
        obj["sdate_rss_tp"],
        obj["comments"],
        obj["frame"],
        obj["like_count"],
        obj["description"],
        obj["is_deleted"],
        row["update_count"],
    )


def creating_table_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("CREATE DATABASE IF NOT EXISTS bronze;")
    hook.execute("DROP TABLE IF EXISTS bronze.videos;")
    hook.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.videos (
            
            _id                     String,
            created_at              TIMESTAMP,
            expire_at               TIMESTAMP,
            is_produce_to_kafka     BOOLEAN,
            platform                String,
            id                      String,
            owner_username          String,
            owner_id                String,
            title                   String,
            tags                    String,
            uid                     String,
            visit_count             Integer,
            owner_name              String,
            poster                  String,
            owner_avatar            String,
            duration                Integer,
            posted_date             TIMESTAMP,
            posted_timestamp        INTEGER,
            sdate_rss               TIMESTAMP,
            sdate_rss_tp            INTEGER,
            comments                String,
            frame                   String,
            like_count              INTEGER,
            description             String,
            is_deleted              BOOLEAN,
            update_count            Integer
        ) ENGINE = MergeTree()
          ORDER BY _id;
        """
    )


def creating_table_clickhouse():
    hook = get_conn_clickhouse()
    hook.execute("CREATE DATABASE IF NOT EXISTS bronze;")
    hook.execute("DROP TABLE IF EXISTS bronze.videos;")
    hook.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze.videos (
            
            _id                     String,
            created_at              TIMESTAMP,
            expire_at               TIMESTAMP,
            is_produce_to_kafka     BOOLEAN,
            object                  String,
            update_count            Integer
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
