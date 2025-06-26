"""
data_generator.py

This module provides utility functions to generate fake dummy data
for customers, products, and purchases using the `faker` library.
All generated timestamps are based on current time in Asia/Jakarta
and converted to UTC.

Functions:
    - generate_customers
    - generate_products
    - generate_purchases
"""

from faker import Faker
from datetime import datetime
from typing import List, Tuple
import random
import pytz
import logging
from utils.logger import setup_logger

# Initialize Faker and logger
fake = Faker()
logger = setup_logger()

now_local = datetime.now(pytz.timezone("Asia/Jakarta"))
now_utc = now_local.astimezone(pytz.utc)

def generate_customers(n: int = 5) -> List[Tuple[str, str, str, str, str, str]]:
    """
    Generates a list of dummy customer records.

    Each customer record contains:
        - name (str)
        - email (str)
        - phone (str)
        - address (str)
        - created_at (str): UTC timestamp
        - updated_at (str): UTC timestamp

    Args:
        n (int, optional): Number of customer records to generate. Defaults to 5.

    Returns:
        List[Tuple[str, str, str, str, str, str]]:
            A list of tuples, each representing a customer record.
    """
    # Log current time
    logger.info(f"[DATA GENERATOR] Current time (UTC+0): {now_utc}")
    return [(fake.name(), fake.email(), fake.phone_number()[:18], fake.address(), now_utc, now_utc) for _ in range(n)]

def generate_products(n: int = 5) -> List[Tuple[str, str, float, int, str, str]]:
    """
    Generates a list of dummy product records.

    Each product record contains:
        - product_name (str)
        - category (str)
        - price (float): random float between 10 and 500
        - stock (int): random integer between 10 and 100
        - created_at (str): UTC timestamp
        - updated_at (str): UTC timestamp

    Args:
        n (int, optional): Number of product records to generate. Defaults to 5.

    Returns:
        List[Tuple[str, str, float, int, str, str]]: 
            A list of tuples, each representing a product record.
    """
    return [(fake.word(), fake.word(), round(random.uniform(10, 500), 2), random.randint(10, 100), now_utc, now_utc) for _ in range(n)]

def generate_purchases(    
    customer_ids: List[int],
    product_ids: List[int],
    n: int = 5
) -> List[Tuple[int, int, int, float, str, str]]:
    """
    Generates a list of dummy purchase records.

    Each purchase record contains:
        - customer_id (int): Randomly selected from provided customer IDs.
        - product_id (int): Randomly selected from provided product IDs.
        - quantity (int): Random integer between 1 and 3.
        - total_price (float): Computed as quantity * random price between 10 and 500.
        - created_at (str): UTC timestamp.
        - updated_at (str): UTC timestamp.

    Args:
        customer_ids (List[int]): List of valid customer IDs to use.
        product_ids (List[int]): List of valid product IDs to use.
        n (int, optional): Number of purchase records to generate. Defaults to 5.

    Returns:
        List[Tuple[int, int, int, float, str, str]]:
            A list of tuples, each representing a purchase record.
    """
    return [
        (
            random.choice(customer_ids),
            random.choice(product_ids),
            q := random.randint(1, 3),
            round(q * random.uniform(10, 500), 2),
            now_utc,
            now_utc
        ) for _ in range(n)
    ]
