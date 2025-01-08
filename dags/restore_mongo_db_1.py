from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import subprocess

# تعریف آرگومان‌های پیش‌فرض DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# تابع برای اجرای فرمان mongorestore
def run_mongorestore():
    # مسیر فایل bson
    bson_file_path = "/data/db/videos.bson"
    
    # پارامترهای اتصال به MongoDB
    mongo_host = "mongo"  # نام کانتینر MongoDB
    mongo_port = "27017"
    mongo_user = "admin"
    mongo_password = "password"
    mongo_auth_db = "admin"
    mongo_db = "youtube-videos-db"
    mongo_collection = "videos"

    # فرمان mongorestore
    command = [
        "mongorestore",
        "--host", mongo_host,
        "--port", mongo_port,
        "--username", mongo_user,
        "--password", mongo_password,
        "--authenticationDatabase", mongo_auth_db,
        "--db", mongo_db,
        "--collection", mongo_collection,
        bson_file_path
    ]

    # اجرای فرمان با استفاده از subprocess
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e.stderr)
        raise

# تعریف DAG
with DAG(
    dag_id='restore_mongo_db_with_subprocess',
    default_args=default_args,
    description='Restore MongoDB Database using mongorestore with SubprocessOperator',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # تعریف Task برای اجرای mongorestore
    restore_mongo_task = PythonOperator(
        task_id='mongorestore_task',
        python_callable=run_mongorestore,
    )

restore_mongo_task
