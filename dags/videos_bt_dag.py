from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


default_args = {
    "owner": "Mkz-Majid",
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
        client = Client('82.115.20.70', port=9000)

        create_db = '''
        CREATE DATABASE IF NOT EXISTS silver;
        '''

        client.execute(create_db)

        # Define the CREATE TABLE query
        create_query = '''
        CREATE TABLE IF NOT EXISTS silver.Videos_channels_OBT (
            video_id Nullable(Int32),
            owner_username Nullable(String),
            video_title Nullable(String),
            video_uid Nullable(String),
            video_visit_count Nullable(Int32),
            owner_name Nullable(String),
            video_duration Nullable(Int32),
            video_posted_date Nullable(DateTime),
            video_description Nullable(String),
            video_is_deleted Nullable(Bool),
            video_update_count Int32,
            channel_userid String,
            channel_bio_links String,
            channel_total_video_visit UInt64,
            channel_video_count UInt64,
            channel_start_date DateTime,
            channel_followers_count UInt64,
            channel_following_count UInt64,
            channel_country String,
            channel_update_count UInt64,
            created_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (channel_userid);
        '''

        client.execute(create_query)

        # Truncate table
        truncate_table_query = '''
        TRUNCATE TABLE silver.Videos_channels_OBT;
        '''

        client.execute(truncate_table_query)

        # Define the INSERT query to transfer data into the new table
        insert_query = '''
        INSERT INTO silver.Videos_channels_OBT
            SELECT
            v.id AS video_id,
            v.owner_username,
            v.title AS video_title,
            v.uid AS video_uid,
            v.visit_count AS video_visit_count,
            v.owner_name,
            v.duration AS video_duration,
            v.posted_date AS video_posted_date,
            v.description AS video_description,
            v.is_deleted AS video_is_deleted,
            v.update_count AS video_update_count,
            c.userid AS channel_userid,
            c.bio_links AS channel_bio_links,
            c.total_video_visit AS channel_total_video_visit,
            c.video_count AS channel_video_count,
            c.start_date AS channel_start_date,
            c.followers_count AS channel_followers_count,
            c.following_count AS channel_following_count,
            c.country AS channel_country,
            c.update_count AS channel_update_count,
            v.created_at AS created_at
        FROM bronze.videos AS v
        LEFT JOIN bronze.channels_new AS c
        ON v.owner_username = c.userid;
        '''

        client.execute(insert_query)

        create_matmv_query = '''
        CREATE MATERIALIZED VIEW IF NOT EXISTS silver.MV_Videos_channels_OBT
        TO silver.Videos_channels_OBT
        AS
        SELECT
            v.id AS video_id,
            v.owner_username,
            v.title AS video_title,
            v.uid AS video_uid,
            v.visit_count AS video_visit_count,
            v.owner_name,
            v.duration AS video_duration,
            v.posted_date AS video_posted_date,
            v.description AS video_description,
            v.is_deleted AS video_is_deleted,
            v.update_count AS video_update_count,
            c.userid AS channel_userid,
            c.bio_links AS channel_bio_links,
            c.total_video_visit AS channel_total_video_visit,
            c.video_count AS channel_video_count,
            c.start_date AS channel_start_date,
            c.followers_count AS channel_followers_count,
            c.following_count AS channel_following_count,
            c.country AS channel_country,
            c.update_count AS channel_update_count,
            v.created_at AS created_at
        FROM bronze.videos AS v
        LEFT JOIN bronze.channels_new AS c
        ON v.owner_username = c.userid;
        '''

        client.execute(create_matmv_query)

    
    
    print_context() >> create_new_ver_channel() >> etl()


videos_bt()