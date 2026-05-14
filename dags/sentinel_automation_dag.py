from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import sys
import os

PROJECT_PATH = os.getenv('AIRFLOW_PROJECT_ROOT', '/opt/airflow/project')
sys.path.append(f'{PROJECT_PATH}/scripts')

from embedder import process_local_to_chroma
from sentinel_bridge import diagnostic_lookup_and_curate

default_args = {
    'owner': 'sentinel_dev',
    'start_date': datetime(2026, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_for_complete_batch():
    base = f'{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing'
    logs_dir = f'{base}/logs'
    notes_dir = f'{base}/notes'
    meta_dir = f'{base}/metadata'

    # All three directories must exist and have at least one matching file
    has_logs = os.path.isdir(logs_dir) and any(f.endswith('.log') for f in os.listdir(logs_dir))
    has_notes = os.path.isdir(notes_dir) and any(f.endswith('.txt') for f in os.listdir(notes_dir))
    has_meta = os.path.isdir(meta_dir) and any(f.endswith('.json') for f in os.listdir(meta_dir))

    return has_logs and has_notes and has_meta

with DAG(
    'sentinel_data_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. WATCHER: Confirm all 3 file types are present before proceeding
    wait_for_batch = PythonSensor(
        task_id='wait_for_complete_sentinel_batch',
        python_callable=check_for_complete_batch,
        poke_interval=30,
        timeout=600
    )

    # 2. INGESTION: Embed notes + logs into ChromaDB
    task_ingestion = PythonOperator(
        task_id='ingestion_layer_processing',
        python_callable=process_local_to_chroma,
        op_kwargs={'base_dir': f'{PROJECT_PATH}/data/landing/sentinel_landing/sentinel_s3_landing'}
    )

    # 3. CURATION: Semantic search → MySQL metadata join → upsert to gold table
    task_curation = PythonOperator(
        task_id='curation_layer_correlation',
        python_callable=diagnostic_lookup_and_curate,
        op_kwargs={'user_query': 'check for performance anomalies'}
    )

    wait_for_batch >> task_ingestion >> task_curation