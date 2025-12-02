from silver.scd2_loader import scd2_load

scd2_load(
    bronze_table="bronze_api_productCategories",
    silver_table="silver_dim_product_categories",
    business_key="productCategoryId"
)
