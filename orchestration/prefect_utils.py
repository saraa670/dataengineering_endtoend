from prefect import task, flow
import os
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

BRONZE_PATH = "bronze/"
SILVER_PATH = "silver/"
GOLD_PATH = "gold/"

@task
def extract_data(domain):
    print(f"[EXTRACT] Extracting raw data for {domain}...")
    return True

@task
def load_to_minio(domain):
    print(f"[MINIO] Uploading raw data for {domain} to MinIO storage...")
    return True

@task
def load_to_bronze(domain):
    print(f"[BRONZE] Converting raw → bronze parquet for {domain}...")
    os.system("python load_parquets_to_postgres.py")
    return True

@task
def transform_to_silver(domain):
    print(f"[SILVER] Running SCD2 + dimensions for {domain}...")
    if domain == "sales":
        os.system("python silver_sales_fact_orders.py")
    if domain == "hr":
        os.system("python scd2_employees.py")
    if domain == "customer":
        os.system("python scd2_customers.py")
    if domain == "production":
        os.system("python scd2_products.py")
    return True

@task
def build_gold_views(domain):
    print(f"[GOLD] Building gold layer for {domain}...")
    os.system("python run_all_gold.py")
    return True

@task
def notify_success(domain):
    print(f"Pipeline for {domain} completed successfully!")
    return True
