# ingestion_data_dag.py
"""
Airflow DAG for daily ETL ingestion of retail data into BigQuery.

This DAG performs an end-to-end ETL (Extract, Transform, Load) process for retail data
on a daily schedule. It handles three tables: `customer`, `products`, and `purchase`.

Each table is processed using a TaskGroup that includes:
    - Extraction from a data source via `extract_data`
    - Transformation logic via `transform_data`
    - Loading the cleaned data into Google BigQuery via `load_to_bigquery`

The DAG is configured to:
    - Run daily (`@daily`)
    - Retry failed tasks once with a 5-minute delay
    - Disable catchup behavior to avoid retroactive runs

Modules used:
    - `extract_data` from `services.extract`
    - `transform_data` from `services.transform`
    - `load_to_bigquery` from `services.load`

Tags: `etl`, `retail`, `bigquery`

DAG ID:
    `retail_daily_ingestion`

TaskGroups:
    - customer_etl
    - products_etl
    - purchase_etl

Each TaskGroup contains:
    - extract_{table_name}
    - transform_{table_name}
    - load_{table_name}_to_bq
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from services.extract import extract_data
from services.transform import transform_data
from services.load import load_to_bigquery

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='retail_daily_ingestion',
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Daily at 1:00 UTC #'@daily',
    catchup=False,
    tags=['etl', 'retail', 'bigquery'],
) as dag:

    table_names = ['customer', 'products', 'purchase']

    for table_name in table_names:
        with TaskGroup(group_id=f"{table_name}_etl", tooltip=f"{table_name.capitalize()} ETL Tasks") as etl_group:
            extract = PythonOperator(
                task_id=f'extract_{table_name}',
                python_callable=extract_data,
                op_kwargs={'table_name': table_name},
            )

            transform = PythonOperator(
                task_id=f'transform_{table_name}',
                python_callable=transform_data,
                op_kwargs={'table_name': table_name},
            )

            load = PythonOperator(
                task_id=f'load_{table_name}_to_bq',
                python_callable=load_to_bigquery,
                op_kwargs={'table_name': table_name},
            )

            extract >> transform >> load