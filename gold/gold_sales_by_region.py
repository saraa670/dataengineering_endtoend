from gold_utils import load_df, write_table, create_view

region = load_df("""
    SELECT
        c."territory" AS region,
        COUNT(*) AS total_orders,
        SUM(s."subtotal") AS revenue
    FROM "silver_sales_fact_orders" s
    LEFT JOIN "silver_dim_customers" c
        ON s."customerId" = c."customerId"
    GROUP BY 1
    ORDER BY revenue DESC;
""")

write_table(region, "gold_sales_by_region")

create_view(
    "vw_gold_sales_by_region",
    "SELECT * FROM gold_sales_by_region"
)
