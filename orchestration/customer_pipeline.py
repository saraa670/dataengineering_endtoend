from prefect import flow
from prefect_utils import *
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

@flow(name="customer_pipeline")
def customer_flow():
    extract_data("customer")
    load_to_minio("customer")
    load_to_bronze("customer")
    transform_to_silver("customer")
    build_gold_views("customer")
    notify_success("customer")

if __name__ == "__main__":
    customer_flow()
