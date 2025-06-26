# services/insert.py
"""
Modules:
    - psycopg2: PostgreSQL client for Python.
    - dotenv: To load environment variables from `.env` file.

Functions:
    run_insertion(query, data_generator, data_args=None)
        Executes bulk insertions into PostgreSQL using a specified query and 
        data generator function.

    insert_customers()
        Inserts dummy customer records into the `customer` table.

    insert_products()
        Inserts dummy product records into the `products` table.

    insert_purchases()
        Inserts dummy purchase records into the `purchase` table by first 
        retrieving existing customer and product IDs.

Environment Variables Required (from `.env`):
    RETAIL_POSTGRES_HOST: Hostname of the PostgreSQL server.
    RETAIL_POSTGRES_DB: Database name.
    RETAIL_POSTGRES_USER: Username for database authentication.
    RETAIL_POSTGRES_PASSWORD: Password for database authentication.

Usage:
    Make sure the `.env` file exists at `/opt/airflow/.env` and contains valid 
    PostgreSQL credentials. Also ensure the `utils/data_generator.py` module 
    exists and contains the required generator functions.
"""

import os
import psycopg2
from typing import Callable, Optional, Tuple, Any
from dotenv import load_dotenv

from utils.data_generator import generate_customers, generate_products, generate_purchases
from utils.logger import setup_logger

logger = setup_logger()

# Load environment variables from .env file
load_dotenv(dotenv_path='/opt/airflow/.env')  # adjust path if needed

DB_CONFIG = {
    'host': os.getenv('RETAIL_POSTGRES_HOST'),
    'dbname': os.getenv('RETAIL_POSTGRES_DB'),
    'user': os.getenv('RETAIL_POSTGRES_USER'),
    'password': os.getenv('RETAIL_POSTGRES_PASSWORD')
}

def run_insertion(
    query: str,
    data_generator: Callable[..., list[tuple]],
    data_args: Optional[Tuple[Any, ...]] = None
) -> None:
    """
    Executes batch insertion of generated data into the PostgreSQL database.

    Args:
        query (str): SQL INSERT query with placeholders (%s).
        data_generator (Callable[..., list[tuple]]): Function to generate data.
        data_args (Optional[Tuple[Any, ...]]): Arguments passed to data generator.

    Raises:
        psycopg2.Error: If the database connection or insertion fails.

    Returns:
        None
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    data = data_generator(*data_args) if data_args else data_generator()

    for row in data:
        cur.execute(query, row)

    # """
    # This line uses a conditional expression (also called a ternary operator) to decide how to call the data_generator function based on whether data_args is provided.

    # data_generator(*data_args):
    # If data_args is not None, this unpacks the tuple data_args into arguments for the function.
    # For example, if data_args = (5,), it becomes data_generator(5).

    # data_generator():
    # If data_args is None or empty, the function is called without any arguments.

    # The result is stored in the variable data, which is expected to be an iterable of rows (e.g., a list of tuples).

    # for row in data:
    # This starts a loop that iterates through each item (row) in the generated data.

    # cur.execute(query, row)
    # This uses the psycopg2 cursor to execute a parameterized INSERT SQL query for each row in the generated data.

    # query is the SQL statement with %s placeholders (e.g., INSERT INTO ... VALUES (%s, %s, %s)).

    # row is a tuple of values that will be bound to those placeholders.

    # This approach is safe against SQL injection and works well for repeated inserts.
    # """

    conn.commit()
    cur.close()
    conn.close()

def insert_customers()-> None:
    """
    Generates and inserts dummy customer records into the `customer` table.

    Each record contains:
        - name
        - email
        - phone
        - address
        - created_at
        - updated_at

    Uses `generate_customers` to produce 3 dummy records.
    """
    query = """
        INSERT INTO customer (name, email, phone, address, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    run_insertion(query, generate_customers, data_args=(3,))

def insert_products()-> None:
    """
    Generates and inserts dummy product records into the `products` table.

    Each record includes:
        - product_name
        - category
        - price
        - stock
        - created_at
        - updated_at

    Uses `generate_products` to produce 5 dummy records.
    """
    query = """
        INSERT INTO products (product_name, category, price, stock, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    run_insertion(query, generate_products, data_args=(5,))

def insert_purchases()-> None:
    """
    Generates and inserts dummy purchase records into the `purchase` table.

    Steps:
        - Fetches all `customer_id` from the `customer` table.
        - Fetches all `product_id` from the `products` table.
        - Generates 5 dummy purchase records using `generate_purchases`.
        - Inserts each record with:
            - customer_id
            - product_id
            - quantity
            - total_price
            - created_at
            - updated_at

    Raises:
        ValueError: If there are no customers or products available in the database.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get customer and product IDs
    cur.execute("SELECT customer_id FROM customer")
    customer_ids = [row[0] for row in cur.fetchall()]
    # """
    # cur.fetchall() returns a list of rows (as tuples) from the executed query.
    # Example: [ (1,), (2,), (3,) ]

    # [row[0] for row in ...] extracts the first item of each tuple (the ID), resulting in:
    # customer_ids = [1, 2, 3]
    # """

    cur.execute("SELECT product_id FROM products")
    product_ids = [row[0] for row in cur.fetchall()]

    if not customer_ids or not product_ids: # This checks if either customer_ids or product_ids is empty.
        logger.error("No customers or products available for purchase generation")
        cur.close()
        conn.close()
        raise ValueError("No customers or products available for purchase generation")

    data = generate_purchases(customer_ids, product_ids, 5)

    query = """
        INSERT INTO purchase (customer_id, product_id, quantity, total_price, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    for row in data:
        cur.execute(query, row)

    conn.commit()
    cur.close()
    conn.close()
