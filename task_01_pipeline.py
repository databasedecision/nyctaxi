import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from modules.data_extraction import extract_data
from modules.data_transformation import transform_data
from modules.data_validation import validate_data
from modules.data_loading import load_data

from airflow.models import Variable
from google.cloud import storage
import os

# Get the current Composer environment's configuration
composer_bucket_name = Variable.get("bucket")
metadata_blob = Variable.get("metadata-blob")

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
with DAG('task_01_pipeline', default_args=default_args, schedule_interval='0 1 * * *', start_date=datetime(2024, 5, 2), catchup=False) as dag:
    
    # Load metadata
    metadata = load_metadata(composer_bucket_name, metadata_blob)
    
    # Extract data task
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
            'source_table': metadata['source_table'],
            'watermark_column': metadata['source_table']['watermark_column'],
            'execution_date': '{{ ds }}',
            'output_path': metadata['extracted_data']
        }
    )

    # Transform data task
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        op_kwargs={
            'transformations': metadata['transformations'],
            'input_path': metadata['extracted_data'],
            'output_path': metadata['transformed_data']
        }
    )
    
    # Load data task
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={
            'destination_table': metadata['destination_table'],
            'input_path': metadata['transformed_data'],
            'bq_schema': metadata['bq_schema']
        }
    )

    # Validate data task
    validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        op_kwargs={
            'source_table': metadata['destination_table'],
            'execution_date': '{{ ds }}',
            'anomaly_threshold': metadata['anomaly_threshold']
        }
    )
    
    # Set task dependencies
    extract_task >> transform_task >> load_data >> validate_data