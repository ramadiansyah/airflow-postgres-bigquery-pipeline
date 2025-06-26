# services/extract.py

import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta
from typing import Any, Dict
from utils.logger import setup_logger

logger = setup_logger()

def extract_data(table_name: str, **kwargs: Dict[str, Any]) -> None:
    """
    Extracts daily data from a PostgreSQL table and saves it as a Parquet file.

    This function queries data from the given table in the PostgreSQL database
    where the `created_at` column matches the previous day's date (`execution_date - 1 day`).
    The result is saved as a Parquet file in the `tmp/` directory.

    Args:
        table_name (str): Name of the table to extract data from.
        **kwargs (Dict[str, Any]): Keyword arguments passed by Airflow context,
            must include 'execution_date' (datetime object).

    Raises:
        KeyError: If 'execution_date' is not found in kwargs.
        sqlalchemy.exc.SQLAlchemyError: If there is a database connection or query issue.
        OSError: If file write fails (e.g., permission issues).
    """
    h_minus_1 = (kwargs['execution_date'] - timedelta(days=0)).date()
    query = f"SELECT * FROM {table_name} WHERE DATE(created_at) = '{h_minus_1}'"

    logger.info(f"query: {query}")

    engine = create_engine(
        f"postgresql://{os.getenv('RETAIL_POSTGRES_USER')}:{os.getenv('RETAIL_POSTGRES_PASSWORD')}@retail-db:5432/{os.getenv('RETAIL_POSTGRES_DB')}"
    )

    df = pd.read_sql(query, engine)
    # Ensure 'tmp' directory exists
    os.makedirs("tmp", exist_ok=True)

    filename = f"{table_name}_extracted_{h_minus_1}"
    df.to_parquet(f"tmp/{filename}.parquet", index=False)

