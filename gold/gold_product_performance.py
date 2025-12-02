from gold_utils import load_df, write_table, create_view

performance = load_df("""
    SELECT
        p."productId",
        p."name" AS product_name,
        SUM(s."quantity") AS total_quantity,
        SUM(s."subtotal") AS total_revenue
    FROM "silver_sales_fact_orders" s
    LEFT JOIN "silver_dim_products" p
        ON s."productId" = p."productId"
    GROUP BY 1,2
    ORDER BY total_revenue DESC;
""")

write_table(performance, "gold_product_performance")

create_view(
    "vw_gold_product_performance",
    "SELECT * FROM gold_product_performance"
)
