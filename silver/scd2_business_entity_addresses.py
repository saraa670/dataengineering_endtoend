from silver.scd2_loader import scd2_load

scd2_load(
    bronze_table="bronze_api_businessEntityAddresses",
    silver_table="silver_dim_business_entity_addresses",
    business_key="businessEntityId"
)
