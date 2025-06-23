import boto3
import pandas as pd
from io import StringIO
import os

from pathlib import Path
import sys

# Add the project root to sys.path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from scripts.processing.transform import transform_stock_symbols

# Define S3 bucket and key prefix
BUCKET_NAME = "snowflake1bucketdatawh"
KEY_PREFIX = "csv/transformed-bulk-data/"

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-north-1"
)

# Upload function
def upload_dataframe_to_s3(df, file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_key = f"{KEY_PREFIX}{file_name}"
    
    s3.put_object(
        Bucket=BUCKET_NAME, 
        Key=file_key, 
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f"Uploaded {file_name} to {BUCKET_NAME}/{file_key}")

def upload_stock_symbols_s3(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_symbols', key='transformed_stock_symbols')
    upload_dataframe_to_s3(data, "stock_symbols_bulk.csv")

def upload_global500_s3(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_global500', key='transformed_global500')
    upload_dataframe_to_s3(data, "global500_bulk.csv")

def upload_stock_s3(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_stock_data', key='transformed_stock')
    upload_dataframe_to_s3(data, "stock_data_bulk.csv")
