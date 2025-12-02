import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:sa11%4022@localhost:5432/adventureworks")

silver_dims = [
    "silver_dim_customers",
    "silver_dim_products",
    "silver_dim_employees",
    "silver_dim_locations"
]

for table in silver_dims:
    print(f"Cleaning {table}")

    df = pd.read_sql(f'select * from "{table}"', engine)

    # Example cleaning rules:
    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

    if "phone" in df.columns:
        df["phone"] = df["phone"].str.replace(" ", "").str.replace("-", "")

    date_cols = [c for c in df.columns if "date" in c.lower()]
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors='coerce')

    # write back
    df.to_sql(table, engine, if_exists="replace", index=False)

print("Silver cleaning complete!")
