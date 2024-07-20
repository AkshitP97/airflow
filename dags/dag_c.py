# dag_c.py

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datasets import dataset_c
from datetime import datetime

default_args = {
    'owner': 'Akshit',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'dag_c',
    default_args=default_args,
    schedule_interval='15 13 18-20 * *',
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id='start',
    )

    update_dataset = EmptyOperator(
        task_id='update_dataset',
        outlets=[dataset_c],
    )

    start >> update_dataset