from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


default_args = {
    "owner": "Mkz",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id='videos_bt',
    description='a dag to transform data and create videos big table',
    schedule='@once',
    default_args=default_args,
    catchup=False,
    tags=['clickhouse', 'big_table']        
)
def videos_bt():
    from pprint import pprint

    clickhouse_engine = ClickHouseHook(clickhouse_conn_id='clickhouse_tcp')    


    @task
    def print_context(**context):
        pprint(context)

    @task
    def create_or_replace_table():
        clickhouse_engine.execute(
            '''
                select 1;
            '''
        )

    @task
    def etl():
        clickhouse_engine.execute(
            '''
                select 1;
            '''
        )
    
    
    print_context() >> create_or_replace_table() >> etl()


videos_bt()