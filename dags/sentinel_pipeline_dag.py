import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = os.getenv("AIRFLOW_PROJECT_ROOT", "/opt/airflow/project")

default_args = {
    'owner': 'sentinel',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sentinel_pipeline',
    default_args=default_args,
    description='Sentinel ETL: Migrate → Curate → Analytics',
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    catchup=False,
    tags=['sentinel', 'etl'],
)

# Task 1: Run database migrations
task_migrate = BashOperator(
    task_id='db_migrate',
    bash_command=f'cd {PROJECT_ROOT} && python -m scripts.db_migrate',
    dag=dag,
)

# Task 2: Run curation transform
task_curation = BashOperator(
    task_id='curation_transform',
    bash_command=f'cd {PROJECT_ROOT} && python -m src.curation.curation_transform',
    dag=dag,
)

# Task 3: Populate analytics
task_analytics = BashOperator(
    task_id='populate_analytics',
    bash_command=f'cd {PROJECT_ROOT} && python -m src.curation.populate_sentinel_star_schema',
    dag=dag,
)

# Define dependencies
task_migrate >> task_curation >> task_analytics
