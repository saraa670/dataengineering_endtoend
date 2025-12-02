import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

DB_USER = "postgres"
DB_PASS = "sa11%4022"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "adventureworks"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DB_URL)

print("\n=== Building Silver Fact Table: silver_sales_fact_orders ===\n")

# ---- Load Dimensions ----
products = pd.read_sql("SELECT * FROM silver_dim_products WHERE is_current = TRUE", engine)
customers = pd.read_sql("SELECT * FROM silver_dim_customers WHERE is_current = TRUE", engine)
employees = pd.read_sql("SELECT * FROM silver_dim_employees WHERE is_current = TRUE", engine)

if products.empty or customers.empty or employees.empty:
    print("Missing dimension data. Cannot build fact table.")
    exit()

# ---- Synthetic Sales Generation ----
num_rows = 500  # create 500 fake sales

fact = pd.DataFrame({
    "salesOrderId": range(1, num_rows + 1),
    "productId": np.random.choice(products["productId"], num_rows),
    "customerId": np.random.choice(customers["customerId"], num_rows),
    "employeeId": np.random.choice(employees["employeeId"], num_rows),
    "orderDate": pd.date_range(start="2023-01-01", periods=num_rows).astype(str),
    "quantity": np.random.randint(1, 10, num_rows),
})

# Add unitPrice from products table
fact = fact.merge(products[["productId", "listPrice"]], on="productId", how="left")
fact["unitPrice"] = fact["listPrice"]
fact["subtotal"] = fact["unitPrice"] * fact["quantity"]

# Drop unneeded temp field
fact.drop(columns=["listPrice"], inplace=True)

# ---- Write to PostgreSQL ----
fact.to_sql("silver_sales_fact_orders", engine, index=False, if_exists="replace")

print("Created silver_sales_fact_orders with rows:", len(fact))
print("Done.\n")
