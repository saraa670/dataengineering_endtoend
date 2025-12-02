from prefect import flow
from prefect_utils import *
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

@flow(name="production_pipeline")
def production_flow():
    extract_data("production")
    load_to_minio("production")
    load_to_bronze("production")
    transform_to_silver("production")
    build_gold_views("production")
    notify_success("production")

if __name__ == "__main__":
    production_flow()
