from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.etl.weather_etl import run_etl

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="run_weather_etl",
        python_callable=run_etl
    )