from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from amazon_etl import run_amazon_etl
from amazon_etl import uploadtos3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    
}

mydag = DAG(
    'laptop-data-dag',
    default_args=default_args,
    description='Airflow DAG with ETL process!',
    schedule="@weekly",
)

run_etl = PythonOperator(
    task_id='fetch_data',
    python_callable=run_amazon_etl,
    dag=mydag
)
run_etl
