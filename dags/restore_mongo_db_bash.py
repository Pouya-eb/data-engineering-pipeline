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
            #'docker exec -i mongo sh -c mongorestore --username admin --password password --authenticationDatabase admin --db mydb --collection videos /data/db/videos.bson'
            'docker exec -i mongo sh -c "mongorestore --username admin --password password --authenticationDatabase admin --db mydb --collection videos /data/db/videos.bson"'

             #docker exec -i <mongodb container> sh -c 'mongorestore --authenticationDatabase admin -u <user> -p <password> --db <database> --archive' < db.dump
        
        )
    )

restore_mongo