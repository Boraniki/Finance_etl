from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pathlib import Path
import sys

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Import tasks
from scripts.api.extract import get_forbes_global_500
from scripts.processing.transform import transform_forbes_global_500
from scripts.loading.loading_s3 import upload_global500_s3


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

# Define DAG
with DAG(
    'global500_data_pipline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_global500 = PythonOperator(
        task_id="extract_global500",
        python_callable=get_forbes_global_500,
        provide_context=True,
    )

    # Task 2: Transform data
    transform_global500 = PythonOperator(
        task_id="transform_global500",
        python_callable=transform_forbes_global_500,
        provide_context=True,
    )

    # Task 3: Load data into Snowflake
    load_global500_to_s3 = PythonOperator(
        task_id="load_global500_to_s3",
        python_callable=upload_global500_s3,
        provide_context=True,
    )

    # Define the pipeline order
    extract_global500 >> transform_global500 >> load_global500_to_s3
