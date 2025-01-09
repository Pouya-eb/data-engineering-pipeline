from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def decide_to_restore(**context):
    """
    Decide whether to run mongorestore based on the output of check_db_and_collection task.
    """
    check_output = context['ti'].xcom_pull(task_ids='check_db_and_collection')
    if "Collection exists" in check_output:
        print("Collection already exists. Skipping restore.")
        return "skip_restore"
    else:
        print("Collection does not exist. Proceeding with restore.")
        return "restore_mongo"

with DAG(
    'restore_mongo_db_with_branch',
    default_args=default_args,
    description='Check MongoDB for database and collection, then restore if not present',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    check_db_and_collection = BashOperator(
        task_id='check_db_and_collection',
        bash_command=(
            '''
            docker exec -i mongo mongo --quiet --eval '
            var db = connect("mongodb://admin:password@localhost:27017/admin");
            if (db.getSiblingDB("mydb").getCollection("videos").count() > 0) {
                print("Collection exists");
            } else {
                print("Collection does not exist");
            }
            '
            '''
        ),
        do_xcom_push=True,  # ذخیره خروجی در XCom
    )

    decide_task = BranchPythonOperator(
        task_id='decide_to_restore',
        python_callable=decide_to_restore,
        provide_context=True,
    )

    restore_mongo = BashOperator(
        task_id='restore_mongo',
        bash_command=(
            'docker exec -i mongo sh -c "mongorestore --username admin --password password --authenticationDatabase admin --db mydb --collection videos /data/db/videos.bson"'
            #'docker exec -i mongo sh'
        ),
    )

    skip_restore = DummyOperator(
        task_id='skip_restore'
    )

    end_task = DummyOperator(
        task_id='end'
    )

    # Dependencies
    check_db_and_collection >> decide_task
    decide_task >> restore_mongo >> end_task
    decide_task >> skip_restore >> end_task
