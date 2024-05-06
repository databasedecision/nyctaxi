import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from modules.data_extraction import extract_data
from modules.data_transformation import transform_data
from modules.data_validation import validate_data
from modules.data_loading import load_data

from airflow.models import Variable
from google.cloud import storage
import os

# Get the current Composer environment's configuration
composer_bucket_name = Variable.get("bucket")
metadata_blob = Variable.get("metadata-blob-task2")

# Function to load metadata from GCS
def load_metadata(composer_bucket_name, metadata_blob):
    client = storage.Client()
    bucket = client.get_bucket(composer_bucket_name) 
    blob = bucket.blob(metadata_blob) 
    metadata_str = blob.download_as_string().decode("utf-8")
    return yaml.safe_load(metadata_str)

# Define default arguments for the DAG
default_args = {
    'owner': 'Gazala',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1: Data Extraction and Transformation
with DAG('task_02_pipeline', default_args=default_args, schedule_interval='0 1 * * *', start_date=datetime(2024, 5, 3)) as dag:
    
    # Load metadata
    metadata = load_metadata(composer_bucket_name, metadata_blob)
    
    wait_for_pipeline_01 = ExternalTaskSensor(
        task_id='_wait_for_pipeline_01',
        external_dag_id='task_01_pipeline',
        external_task_id='validate_data',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        timeout=60*60
    )
    
    for pipeline in metadata['pipelines']:
        # Transform data task
        transform_task = PythonOperator(
            task_id=f"{pipeline['name']}_transform_data",
            python_callable=transform_data,
            op_kwargs={
                'transformations': pipeline['transformations'],
                'input_path': 'ignore',
                'source_table': pipeline['source_table'],
                'pipeline_name': pipeline['name'],
                'execution_date': '{{ ds }}'
            }
        )
        
        # Load data task
        load_task = PythonOperator(
            task_id=f"{pipeline['name']}_load_data",
            python_callable=load_data,
            op_kwargs={
                'destination_table': pipeline['destination_table'],
                'input_path': 'auto',
                'pipeline_name': pipeline['name'],
                'bq_schema': pipeline['bq_schema'],
                'partition': pipeline['partition']
            }
        )
    
        # Set task dependencies
        wait_for_pipeline_01 >> transform_task >> load_task