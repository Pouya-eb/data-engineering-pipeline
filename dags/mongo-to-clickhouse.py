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

        for doc in batch:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        hook.execute("INSERT INTO bronze.videos VALUSE", batch)


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
    schedule="@monthly",
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
