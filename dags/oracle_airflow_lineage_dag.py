from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import logging

# Define your Oracle source and destination tables
ORACLE_SOURCE_TABLES = ["BSHKT_BSH.TABLE1", "BSHKT_BSH.TABLE2"]
ORACLE_DESTINATION_TABLES = ["BSHKT_BSH.TABLE3", "BSHKT_BSH.TABLE4"]

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG that will demonstrate lineage
dag = DAG(
    'oracle_etl_lineage_example',
    default_args=default_args,
    description='Example ETL process with Oracle lineage',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['oracle', 'etl', 'lineage', 'example']
)

# Function to simulate ETL process
def process_oracle_data(**kwargs):
    """Simulate processing data from source to destination tables"""
    logging.info(f"Processing data from {ORACLE_SOURCE_TABLES} to {ORACLE_DESTINATION_TABLES}")
    # In a real scenario, you would have actual ETL logic here
    return True

# Create task
process_task = PythonOperator(
    task_id='process_oracle_data',
    python_callable=process_oracle_data,
    provide_context=True,
    dag=dag,
)

# Add inlets and outlets for lineage
process_task.inlets = [
    {"tables": ORACLE_SOURCE_TABLES}
]
process_task.outlets = [
    {"tables": ORACLE_DESTINATION_TABLES}
]