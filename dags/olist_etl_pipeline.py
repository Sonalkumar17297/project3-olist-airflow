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
    doc_md="Olist E-commerce ETL Pipeline",
    default_args={
        "owner": "Sonal",
        "retries": 1,
        "retry_delay": duration(seconds=30),
    },
    tags=["olist", "ETL", "project3"],
    is_paused_upon_creation=False,
)
def olist_etl_pipeline():

    @task
    def extract():
        t_log.info("Extracting Olist CSV files...")
        orders = pd.read_csv(f"{DATA_PATH}olist_orders_dataset.csv")
        items = pd.read_csv(f"{DATA_PATH}olist_order_items_dataset.csv")
        customers = pd.read_csv(f"{DATA_PATH}olist_customers_dataset.csv")
        payments = pd.read_csv(f"{DATA_PATH}olist_order_payments_dataset.csv")
        t_log.info(f"Orders: {len(orders)} rows")
        t_log.info(f"Items: {len(items)} rows")
        t_log.info(f"Customers: {len(customers)} rows")
        t_log.info(f"Payments: {len(payments)} rows")
        return {
            "orders": orders.to_json(),
            "items": items.to_json(),
            "customers": customers.to_json(),
            "payments": payments.to_json()
        }

    @task
    def validate(data: dict):
        t_log.info("Validating data...")
        orders = pd.read_json(data["orders"])
        
        # Check nulls
        null_count = orders["order_id"].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Found {null_count} null order_ids!")
        
        # Check duplicates
        dup_count = orders["order_id"].duplicated().sum()
        if dup_count > 0:
            raise ValueError(f"Found {dup_count} duplicate order_ids!")
        
        t_log.info("Validation passed!")
        return data

    @task
    def transform(data: dict):
        t_log.info("Transforming data...")
        orders = pd.read_json(data["orders"])
        items = pd.read_json(data["items"])
        customers = pd.read_json(data["customers"])
        payments = pd.read_json(data["payments"])

        # Filter delivered orders only
        orders = orders[orders["order_status"] == "delivered"]

        # Join tables
        df = orders.merge(items, on="order_id", how="left")
        df = df.merge(customers, on="customer_id", how="left")
        df = df.merge(payments, on="order_id", how="left")

        # Select relevant columns
        df = df[[
            "order_id",
            "customer_state",
            "order_purchase_timestamp",
            "payment_value",
            "price"
        ]].drop_duplicates(subset=["order_id"])

        # Add pipeline run date
        df["pipeline_run_date"] = pd.Timestamp.today().strftime("%Y-%m-%d")

        # Extract order month
        df["order_month"] = pd.to_datetime(
            df["order_purchase_timestamp"]
        ).dt.to_period("M").astype(str)

        t_log.info(f"Transformed {len(df)} delivered orders")
        return df.to_json()

    @task
    def report(transformed_data: str):
        t_log.info("Generating sales summary report...")
        df = pd.read_json(transformed_data)

        summary = df.groupby(["customer_state", "order_month"]).agg(
            total_revenue=("payment_value", "sum"),
            order_count=("order_id", "count"),
            avg_order_value=("payment_value", "mean")
        ).reset_index()

        os.makedirs(OUTPUT_PATH, exist_ok=True)
        summary.to_csv(f"{OUTPUT_PATH}sales_summary.csv", index=False)
        t_log.info(f"Report saved — {len(summary)} rows")
        return transformed_data

    @task
    def notify(transformed_data: str):
        df = pd.read_json(transformed_data)
        t_log.info("Pipeline completed successfully!")
        t_log.info(f"Total delivered orders loaded: {len(df)}")
        t_log.info(f"Pipeline run date: {pd.Timestamp.today().strftime('%Y-%m-%d')}")

    # Set task dependencies
    extracted = extract()
    validated = validate(extracted)
    transformed = transform(validated)
    report_out = report(transformed)
    chain(extracted, validated, transformed, report_out, notify(report_out))

olist_etl_pipeline()