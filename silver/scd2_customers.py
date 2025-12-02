from silver.scd2_loader import scd2_load

scd2_load(
    bronze_table="bronze_api_customers",
    silver_table="silver_dim_customers",
    business_key="customerId"
)
