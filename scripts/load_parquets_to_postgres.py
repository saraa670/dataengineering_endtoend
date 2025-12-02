import os
import pandas as pd
from sqlalchemy import create_engine

# ---- UPDATE THIS WITH YOUR POSTGRES DETAILS ----
DB_USER = "postgres"
DB_PASS = "sa11%4022"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "adventureworks"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

BRONZE_PATH = r"C:\Users\SARA\Downloads\dataengineering-project\bronze"

print(f"Searching for parquet files in: {BRONZE_PATH}")
parquet_files = []

# Walk through all nested folders
for root, dirs, files in os.walk(BRONZE_PATH):
    for file in files:
        if file.endswith(".parquet"):
            parquet_files.append(os.path.join(root, file))

print("\nFound Parquet Files:")
for f in parquet_files:
    print(f)

if not parquet_files:
    print("\n No parquet files found! Check folder structure.")
    exit()

# Load each parquet file into Postgres
for parquet_path in parquet_files:
    df = pd.read_parquet(parquet_path)

    # table name = folder name (api_employees, api_products...)
    table_name = parquet_path.split("\\")[-3]   # <--- auto-detect folder name

    table_name = f"bronze_{table_name}"

    print(f"\n Loading {parquet_path} into table {table_name}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)

print("\n All parquet files loaded successfully!")
