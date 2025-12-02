import subprocess
import os

print("\n========= RUNNING GOLD LAYER =========\n")

# Correct folder path
gold_folder = os.path.join(os.getcwd(), "gold")

scripts = [
    "gold_monthly_summary.py",
    "gold_sales_by_category.py",
    "gold_sales_by_region.py",
    "gold_hr_headcount.py",
    "gold_product_performance.py",
    "gold_customer_lifetime.py"
]

for script in scripts:
    script_path = os.path.join(gold_folder, script)

    print(f"\n--- Running {script} ---")

    if os.path.exists(script_path):
        subprocess.run(["python", script_path])
    else:
        print(f"ERROR: File not found → {script_path}")

print("\n========= GOLD LAYER COMPLETE =========")
