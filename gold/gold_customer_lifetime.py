from gold_utils import load_df, write_table, create_view

clv = load_df("""
    SELECT
        c."customerId",
        COUNT(DISTINCT s."salesOrderId") AS total_orders,
        SUM(s."subtotal") AS total_spend,
        MIN(CAST(s."orderDate" AS date)) AS first_order,
        MAX(CAST(s."orderDate" AS date)) AS last_order
    FROM "silver_sales_fact_orders" s
    LEFT JOIN "silver_dim_customers" c
        ON s."customerId" = c."customerId"
    GROUP BY 1;
""")

write_table(clv, "gold_customer_lifetime")

create_view(
    "vw_gold_customer_lifetime",
    "SELECT * FROM gold_customer_lifetime"
)
