from gold_utils import load_df, write_table, create_view

monthly = load_df("""
    SELECT
        date_trunc('month', CAST("orderDate" AS timestamp))::date AS month,
        COUNT(*) AS total_orders,
        SUM("subtotal") AS total_revenue,
        AVG("subtotal") AS avg_order_value
    FROM "silver_sales_fact_orders"
    GROUP BY 1
    ORDER BY 1;
""")

write_table(monthly, "gold_sales_monthly_summary")

create_view(
    "vw_gold_sales_monthly_summary",
    "SELECT * FROM gold_sales_monthly_summary"
)
