from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('D:/retailSalesPipeline/scripts')
import etl_script

default_args = {
    'owner': 'koushik',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('retail_etl_pipeline',
         start_date=datetime(2025, 4, 1),
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args) as dag:

    run_etl = PythonOperator(
        task_id='run_etl_script',
        python_callable=etl_script.run
    )

    run_etl
