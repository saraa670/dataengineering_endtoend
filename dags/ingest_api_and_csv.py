# dags/ingest_api_and_csv.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'sara',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('adventureworks_ingest', start_date=datetime(2025,1,1), schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    extract_csv = BashOperator(
        task_id='extract_csv',
        bash_command='python /opt/airflow/scripts/extract_csv_to_minio.py'
    )

    extract_api = BashOperator(
        task_id='extract_api',
        bash_command='python /opt/airflow/scripts/extract_api_to_minio.py'
    )

    extract_csv >> extract_api
