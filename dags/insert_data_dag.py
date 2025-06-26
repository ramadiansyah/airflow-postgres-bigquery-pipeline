# insert_data_dag.py
"""
DAG Script for Hourly Retail Data Insertion into PostgreSQL

This Airflow DAG is responsible for generating and inserting dummy retail data 
into a PostgreSQL database on an hourly schedule. It covers customer, product, 
and purchase data for testing or prototyping retail data pipelines.

Modules:
    - airflow: Used to define and schedule the DAG and its tasks.
    - datetime: Handles date and time operations.

DAG Tasks:
    - insert_customers: Inserts customer data.
    - insert_products: Inserts product data.
    - insert_purchases: Inserts purchase data after customer and product insertion.

Execution:
    This DAG is scheduled to run every hour and does not perform catchup runs.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from services.insert import insert_customers, insert_products, insert_purchases

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='retail_hourly_insert',
    default_args=default_args,
    start_date=datetime(2025, 6, 10),
    schedule_interval='@hourly',
    catchup=False,
    tags=['etl', 'retail', 'postgres'],
) as dag:

    task_insert_customers = PythonOperator(
        task_id='insert_customers',
        python_callable=insert_customers
    )

    task_insert_products = PythonOperator(
        task_id='insert_products',
        python_callable=insert_products
    )

    task_insert_purchases = PythonOperator(
        task_id='insert_purchases',
        python_callable=insert_purchases
    )

    [task_insert_customers, task_insert_products] >> task_insert_purchases

