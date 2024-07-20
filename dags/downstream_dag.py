# downstream_dag.py

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datasets import dataset_a, dataset_b, dataset_c
from datetime import datetime

default_args = {
    'owner': 'Akshit',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'downstream_dag',
    default_args=default_args,
    schedule=[dataset_a, dataset_b, dataset_c],
    catchup=False,
) as dag:
    
    start = EmptyOperator(
        task_id='start',
    )

    start
