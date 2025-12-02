from silver.scd2_loader import scd2_load

scd2_load(
    bronze_table="bronze_api_employees",
    silver_table="silver_dim_employees",
    business_key="employeeId"
)
