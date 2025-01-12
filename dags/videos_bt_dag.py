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
    from clickhouse_driver import Client
    clickhouse_engine = ClickHouseHook(clickhouse_conn_id='clickhouse_tcp')    


    @task
    def print_context(**context):
        pprint(context)

    @task
    def create_new_ver_channel():
        client = Client('82.115.20.70', port=9000)

        drop_query = '''
        DROP TABLE IF EXISTS bronze.channels_new;
        '''

        client.execute(drop_query)

        # Define the CREATE TABLE query
        create_query = '''
        CREATE TABLE bronze.channels_new
        (
            userid String,
            bio_links String,
            total_video_visit UInt64,
            video_count UInt64,
            start_date DateTime,
            followers_count UInt64,
            following_count UInt64,
            country String,
            update_count UInt64
        ) ENGINE = MergeTree()
        ORDER BY (userid);
        '''

        client.execute(create_query)

        # Define the INSERT query to transfer data into the new table
        insert_query = '''
        INSERT INTO bronze.channels_new
        WITH
        videos_cte AS (
            SELECT 
                v.owner_username AS userid,
                NULL AS bio_links,
                SUM(v.visit_count) AS total_video_visit,
                COUNT(*) AS video_count,
                MIN(v.posted_date) AS start_date,
                NULL AS followers_count,
                NULL AS following_count,
                NULL AS country,
                NULL AS update_count
            FROM bronze.videos AS v
            WHERE v.owner_username NOT IN (
                SELECT c.userid 
                FROM bronze.channels AS c
            )
            GROUP BY v.owner_username
        ),
        channels_cte AS (
            SELECT 
                c.userid AS userid,
                c.bio_links AS bio_links,
                NULL AS total_video_visit,
                NULL AS video_count,
                NULL AS start_date,
                c.followers_count AS followers_count,
                c.following_count AS following_count,
                c.country AS country,
                c.update_count AS update_count
            FROM bronze.channels AS c
        ),
        combined_cte AS (
            SELECT * FROM videos_cte
            UNION ALL
            SELECT * FROM channels_cte
        )
        SELECT *
        FROM combined_cte;
        '''

        client.execute(insert_query)

        print("Table 'channels_new' has been created and data transferred.")

    @task
    def etl():
        clickhouse_engine.execute(
            '''
                select 1;
            '''
        )
    
    
    print_context() >> create_new_ver_channel() >> etl()


videos_bt()