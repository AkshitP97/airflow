from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from google.cloud import storage
import logging

# Define your DAG
default_args = {
    'owner': 'Akshit-GCS',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def extract_trailer_info(bucket_name, object_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    data = blob.download_as_string().decode('utf-8')
    last_line = data.strip().split('\n')[-1]
    trailer_info = last_line.split(',')
    # Process trailer_info as needed
    # Raise an exception if the file is faulty (e.g., missing required data)
    if len(trailer_info) < 7:  # Replace `expected_length` with the actual expected length
        raise ValueError("Faulty file: insufficient trailer information")
    return trailer_info

def process_trailer_info(**context):
    bucket_name = 'test-csv-ingress'
    object_name = 'ingress_folder/test.csv'
    trailer_info = extract_trailer_info(bucket_name, object_name)
    context['ti'].xcom_push(key='trailer_info', value=trailer_info)

with DAG(
    'gcs_to_bigquery_with_archive1',
    default_args=default_args,
    description='A DAG to move CSV files from GCS to BigQuery, process trailer, and archive the files.',
    schedule_interval='@daily',  # No schedule, will be triggered by the sensor
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Sensor to check for the presence of a new file in the ingress folder
    check_for_new_file = GCSObjectExistenceSensor(
        task_id='check_for_new_file',
        bucket='test-csv-ingress',
        object='ingress_folder/test.csv',
        timeout=600,  # Timeout in seconds (e.g., 10 minutes)
        poke_interval=60,  # Check every minute
        mode='poke',
    )

    # Task to extract trailer info from CSV
    extract_trailer = PythonOperator(
        task_id='extract_trailer',
        provide_context=True,
        python_callable=process_trailer_info,
    )

    # Task to load CSV data to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='test-csv-ingress',
        source_objects=['ingress_folder/test.csv'],
        destination_project_dataset_table='aki-learns-slow-69.DBT_AIRFLOW_TEST_DATA.cln_table',
        write_disposition='WRITE_APPEND',
    )

    # Task to move processed CSV to archival folder if successful
    move_to_archival = GCSToGCSOperator(
        task_id='move_to_archival',
        source_bucket='test-csv-ingress',
        source_object='ingress_folder/test.csv',
        destination_bucket='test-csv-ingress',
        destination_object='archival_folder/test.csv',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task to move faulty CSV to faulty folder
    move_to_faulty = GCSToGCSOperator(
        task_id='move_to_faulty',
        source_bucket='test-csv-ingress',
        source_object='ingress_folder/test.csv',
        destination_bucket='test-csv-ingress',
        destination_object='faulty_folder/test.csv',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Task to delete the original CSV file
    delete_original_file = GCSDeleteObjectsOperator(
        task_id='delete_original_file',
        bucket_name='test-csv-ingress',
        objects=['ingress_folder/test.csv'],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define the task dependencies
    check_for_new_file >> extract_trailer >> load_to_bigquery
    load_to_bigquery >> move_to_archival >> delete_original_file
    extract_trailer >> move_to_faulty
