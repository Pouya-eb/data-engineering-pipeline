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
    'restore_mongo_db',
    default_args=default_args,
    description='Restore MongoDB Database using mongorestore',
    schedule_interval=None,  # این DAG دستی اجرا می‌شود
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # اجرای دستور mongorestore
    restore_mongo = BashOperator(
        task_id='mongorestore_task',
        bash_command=(
            'mongorestore --username admin --password password --authenticationDatabase admin --db mydb --collection videos /data/db/videos.bson'
        ),
        #params={
        #    'username': 'admin',
        #    'password': 'password',
        #    'auth_db': 'admin',
        #    'db_name': 'mydb',
        #    'collection_name': 'videos',
        #    'backup_file_path': '/data/db/videos.bson',
        #},
    )

restore_mongo
