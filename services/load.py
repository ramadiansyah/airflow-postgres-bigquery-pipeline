# services/load.py

import os
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import timedelta
from dotenv import load_dotenv

from utils.logger import setup_logger

logger = setup_logger()

load_dotenv(dotenv_path='/opt/airflow/.env')  # adjust path if needed

def load_to_bigquery(table_name: str, **kwargs: dict) -> None:
    """
    Loads transformed Parquet data into BigQuery with upsert (MERGE) logic.

    This function does the following:
        1. Reads the transformed Parquet file based on the previous day's execution date.
        2. Loads the data into a staging table in BigQuery with `WRITE_TRUNCATE` mode.
        3. If the target table doesn't exist, it creates the target table.
        4. Performs a MERGE (upsert) operation from staging to the target table using the ID column.

    Args:
        table_name (str): Name of the table (e.g., 'customer', 'products', 'purchase').
        **kwargs (dict): Airflow context, should include 'execution_date' as a datetime.

    Raises:
        FileNotFoundError: If the transformed Parquet file is not found.
        google.cloud.exceptions.GoogleCloudError: If there's an error during load or query.
    """
    h_minus_1 = (kwargs['execution_date'] - timedelta(days=0)).date()
    filename = f"tmp/{table_name}_extracted_{h_minus_1}_transformed.parquet"

    df = pd.read_parquet(filename)
    logger.info("check df.dtypes:\n", df.dtypes)
    logger.info(f"run_date sample:\n{df['run_date'].head()}")

    # Exit early if no data
    if df.empty:
        logger.warning(f"No data found in file: {filename}. Skipping load to BigQuery.")
        return  # Exit the PythonOperator task without error

    client = bigquery.Client.from_service_account_json(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    
    dataset = os.getenv('RETAIL_BQ_DATASET') #
    target_table = f"{dataset}.{table_name}"
    staging_table = f"{dataset}.stg_{table_name}"

    # Step 1: Load to staging table (overwrite each run)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="run_date"
        ),
    )
    client.load_table_from_dataframe(df, staging_table, job_config=job_config).result()

    # Check if table exists, if not create it
    try:
        client.get_table(target_table)
    except NotFound:
        logger.warning(f"⚠ Table {target_table} not found. Creating it...")

        job_config_target = bigquery.LoadJobConfig(
            write_disposition="WRITE_EMPTY",  # Only create if not exists
            source_format=bigquery.SourceFormat.PARQUET,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="run_date"
            ),
        )
        client.load_table_from_dataframe(df, target_table, job_config=job_config_target).result()

    # Step 2: Run MERGE query for upsert
    id_column = f"{table_name[:-1]}_id" if table_name.endswith('s') else f"{table_name}_id"
    # | `table_name`  | Ends with `'s'`? | `table_name[:-1]` | Resulting `id_column` |
    # | ------------- | ---------------- | ----------------- | --------------------- |
    # | `'products'`  | ✅ Yes            | `'product'`       | `'product_id'`        |
    # | `'purchase'`  | ❌ No             | –                 | `'purchase_id'`       |

    # Create a list of columns from the DataFrame
    columns = df.columns.tolist()

    # Build the UPDATE SET clause, excluding the id_column
    update_set_clause = ",\n  ".join(
        [f"{col} = S.{col}" for col in columns if col != id_column]
    )
    #  example:
    #  name = S.name,
    #  email = S.email,
    #  run_date = S.run_date

    # Build the INSERT clause (columns and VALUES)
    insert_columns = ", ".join(columns)
    # example:
    # columns = ['customer_id', 'name', 'email']
    # insert_columns = "customer_id, name, email"

    insert_values = ", ".join([f"S.{col}" for col in columns])
    # example:
    # insert_values = "S.customer_id, S.name, S.email"

    merge_query = f"""
    MERGE `{target_table}` T
    USING `{staging_table}` S
    ON T.{id_column} = S.{id_column}
    WHEN MATCHED THEN
      UPDATE SET
        {update_set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """
    # Example:
    # MERGE `target_table` T
    # USING `staging_table` S
    # ON T.customer_id = S.customer_id
    # WHEN MATCHED THEN
    # UPDATE SET
    #     name = S.name,
    #     email = S.email
    # WHEN NOT MATCHED THEN
    # INSERT (customer_id, name, email)
    # VALUES (S.customer_id, S.name, S.email)

    logger.info(f"✅ Executing Merge Query: {merge_query}")
    # Runs the SQL statement on BigQuery.
    job = client.query(merge_query)

    # .result() ensures the query is completed before moving on.
    job.result()  # Waits for job to complete

    logger.info(f"✅ Rows affected: {job.num_dml_affected_rows}")
    