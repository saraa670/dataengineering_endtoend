from gold_utils import load_df, write_table, create_view

headcount = load_df("""
    SELECT
        date_trunc('month', "effective_date")::date AS month,
        COUNT(*) FILTER (WHERE "is_current" = TRUE) AS headcount
    FROM "silver_dim_employees"
    GROUP BY 1
    ORDER BY 1;
""")

write_table(headcount, "gold_hr_employee_headcount")

create_view(
    "vw_gold_hr_employee_headcount",
    "SELECT * FROM gold_hr_employee_headcount"
)
