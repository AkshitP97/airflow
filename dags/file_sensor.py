from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

default_args = {
    'owner': 'Akshit',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def file_found():
    logging.info('File detected in GCS bucket!')

with DAG(
    'gcs_file_sensor_example',
    default_args=default_args,
    description='A DAG to sense a specific file in a GCS bucket.',
    schedule_interval='* 2 * * *',  # This DAG is not scheduled, it will only run when triggered manually or by external trigger
    start_date=days_ago(1),
    catchup=False,
) as dag:

    check_for_file = GCSObjectsWithPrefixExistenceSensor(
        task_id='check_for_file',
        bucket='test-csv-ingress',
        prefix='ingress_folder/test.csv',  # Specify the exact path to the file
        timeout=600,  # Timeout in seconds (e.g., 10 minutes)
        poke_interval=5,  # Check every 5 seconds
        mode='poke',  # Adjust mode based on your requirement
    )

    file_found_task = PythonOperator(
        task_id='file_found_task',
        python_callable=file_found,
    )

    check_for_file >> file_found_task
