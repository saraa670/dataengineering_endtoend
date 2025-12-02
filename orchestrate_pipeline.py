from prefect import flow
import subprocess

@flow
def etl_pipeline():
    print("Running Bronze → Postgres Load…")
    subprocess.run(["python", "load_parquets_to_postgres.py"])

    print("Running SCD2 Silver Update…")
    subprocess.run(["python", "bronze_to_silver_scd2.py"])

    print("Running Silver → Gold Transformations…")
    subprocess.run(["python", "silver_to_gold.py"])

if __name__ == "__main__":
    etl_pipeline()
