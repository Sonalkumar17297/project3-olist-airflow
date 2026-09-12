from airflow.sdk import chain, dag, task
from pendulum import datetime, duration
import pandas as pd
import logging
import os

t_log = logging.getLogger("airflow.task")

DATA_PATH = "data/"
OUTPUT_PATH = "output/"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    max_active_runs=1,
    doc_md="Olist E-commerce ETL Pipeline — Project 3",
    default_args={
        "owner": "Sonal",
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["olist", "ETL", "project3"],
    is_paused_upon_creation=False,
    catchup=False,
)
def olist_etl_pipeline():

    @task
    def extract():
        t_log.info("Extracting Olist CSV files...")
        orders = pd.read_csv(f"{DATA_PATH}olist_orders_dataset.csv")
        items = pd.read_csv(f"{DATA_PATH}olist_order_items_dataset.csv")
        customers = pd.read_csv(f"{DATA_PATH}olist_customers_dataset.csv")
        payments = pd.read_csv(f"{DATA_PATH}olist_order_payments_dataset.csv")
        t_log.info(f"Orders: {len(orders)} rows extracted")
        t_log.info(f"Items: {len(items)} rows extracted")