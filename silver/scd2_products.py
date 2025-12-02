from silver.scd2_loader import scd2_load

scd2_load(
    bronze_table="bronze_api_products",
    silver_table="silver_dim_products",
    business_key="productId"
)
