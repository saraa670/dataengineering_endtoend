from prefect import flow
from prefect_utils import *
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

@flow(name="sales_pipeline")
def sales_flow():
    extract_data("sales")
    load_to_minio("sales")
    load_to_bronze("sales")
    transform_to_silver("sales")
    build_gold_views("sales")
    notify_success("sales")

if __name__ == "__main__":
    sales_flow()
