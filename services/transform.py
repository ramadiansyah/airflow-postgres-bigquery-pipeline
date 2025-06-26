# services/transform.py

import pandas as pd
from datetime import timedelta
from typing import Any
from utils.logger import setup_logger

logger = setup_logger()

def transform_data(table_name: str, **kwargs: Any) -> None:
    """
    Transforms extracted data by adding a run date column and removing duplicates.

    This function reads a previously extracted Parquet file from the `tmp/` directory,
    adds a 'run_date' column based on the DAG execution date (h-1),
    removes duplicate rows in-place, and saves the cleaned data to a new transformed
    Parquet file in the same directory.

    Args:
        table_name (str): The name of the table, used to locate the corresponding extracted file.
        **kwargs (Any): Additional keyword arguments passed from Airflow, must include 'execution_date' (datetime).

    Returns:
        None: This function writes the transformed DataFrame to a file and does not return a value.

    Example:
        transform_data(table_name='customer', execution_date=datetime(2025, 6, 15))
    """
    h_minus_1 = (kwargs['execution_date'] - timedelta(days=0)).date()
    filename = f"{table_name}_extracted_{h_minus_1}"
    logger.info(f"filename: {filename}")
    
    df = pd.read_parquet(f"tmp/{filename}.parquet")
    logger.info("check df.dtypes:\n", df.dtypes)

    execution_date = kwargs['execution_date']
    df['run_date'] = execution_date.date()

    logger.info("check df.dtypes:\n", df.dtypes)
    logger.info(f"run_date sample:\n{df['run_date'].head()}")

    df.drop_duplicates(inplace=True)
    df.to_parquet(f"tmp/{table_name}_extracted_{h_minus_1}_transformed.parquet", index=False)
