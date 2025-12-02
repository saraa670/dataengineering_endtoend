from prefect import flow
from prefect_utils import *
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

@flow(name="hr_pipeline")
def hr_flow():
    extract_data("hr")
    load_to_minio("hr")
    load_to_bronze("hr")
    transform_to_silver("hr")
    build_gold_views("hr")
    notify_success("hr")

if __name__ == "__main__":
    hr_flow()
