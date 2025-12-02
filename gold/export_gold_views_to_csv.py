import pandas as pd
from sqlalchemy import create_engine
import os

# Make folder for CSVs
os.makedirs("gold_csvs", exist_ok=True)

# Connect to PostgreSQL
DB_USER = "postgres"
DB_PASS = "sa11%4022"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "adventureworks"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# List of Gold views
gold_views = [
    "vw_gold_sales_monthly_summary",
    "vw_gold_sales_by_region",
    "vw_gold_hr_employee_headcount",
    "vw_gold_product_performance",
    "vw_gold_customer_lifetime"
]

# Export each view to CSV
for view in gold_views:
    df = pd.read_sql(f'SELECT * FROM "{view}"', engine)
    df.to_csv(f"gold_csvs/{view}.csv", index=False)
    print(f"[✔] Exported {view}.csv")
