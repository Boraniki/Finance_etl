from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pathlib import Path
import sys

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Import tasks
from scripts.api.extract import get_all_stock_symbols
from scripts.processing.transform import transform_stock_symbols
from scripts.loading.loading_s3 import upload_stock_symbols_s3


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

# Define DAG
with DAG(
    'symbol_data_pipline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_symbols = PythonOperator(
        task_id="extract_symbols",
        python_callable=get_all_stock_symbols,
        provide_context=True,
    )

    # Task 2: Transform data
    transform_symbols = PythonOperator(
        task_id="transform_symbols",
        python_callable=transform_stock_symbols,
        provide_context=True,
    )

    # Task 3: Load data into Snowflake
    load_symbols_to_s3 = PythonOperator(
        task_id="load_symbols_to_s3",
        python_callable=upload_stock_symbols_s3,
        provide_context=True,
    )

    # Define the pipeline order
    extract_symbols >> transform_symbols >> load_symbols_to_s3
