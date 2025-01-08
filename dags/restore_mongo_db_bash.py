from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# پارامترهای DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'restore_mongo_db_with_bash',
    default_args=default_args,
    description='Restore MongoDB Database using mongorestore with docker exec',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    restore_mongo = BashOperator(
        task_id='mongorestore_task',
        bash_command=(
            'docker exec mongo mongorestore '
            '--username admin'
            '--password password'
            '--authenticationDatabase admin'
            '--db youtube-videos-db'
            '--collection videos'
            '/data/db/videos.bson'
        ),
    )

    restore_mongo
