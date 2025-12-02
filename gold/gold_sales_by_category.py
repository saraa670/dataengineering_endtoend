from gold_utils import load_df, write_table, create_view

category = load_df("""
    SELECT
        pc."name" AS category,
        COUNT(*) AS total_orders,
        SUM(s."subtotal") AS revenue
    FROM "silver_sales_fact_orders" s
    LEFT JOIN "silver_dim_products" p
        ON s."productId" = p."productId"
    LEFT JOIN "silver_dim_productCategories" pc
        ON p."productCategoryId" = pc."productCategoryId"
    GROUP BY 1
    ORDER BY revenue DESC;
""")

write_table(category, "gold_sales_by_category")

create_view(
    "vw_gold_sales_by_category",
    "SELECT * FROM gold_sales_by_category"
)
